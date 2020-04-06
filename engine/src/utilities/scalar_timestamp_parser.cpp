#include <cudf/types.hpp>
#include <cudf/scalar/scalar.hpp>
#include <cudf/scalar/scalar_factories.hpp>
#include <cudf/utilities/type_dispatcher.hpp>
#include <cudf/utilities/traits.hpp>
#include <cudf/wrappers/timestamps.hpp>
#include <vector>
#include <map>

#include "../Utils.cuh"

namespace strings {
namespace detail {
namespace {

/**
 * @brief  Units for timestamp conversion.
 * These are defined since there are more than what cudf supports.
 */
enum class timestamp_units {
  years,           ///< precision is years
  months,          ///< precision is months
  days,            ///< precision is days
  hours,           ///< precision is hours
  minutes,         ///< precision is minutes
  seconds,         ///< precision is seconds
  ms,              ///< precision is milliseconds
  us,              ///< precision is microseconds
  ns               ///< precision is nanoseconds
};


// used to index values in a timeparts array
enum timestamp_parse_component {
  TP_YEAR        = 0,
  TP_MONTH       = 1,
  TP_DAY         = 2,
  TP_HOUR        = 3,
  TP_MINUTE      = 4,
  TP_SECOND      = 5,
  TP_SUBSECOND   = 6,
  TP_TZ_MINUTES  = 7,
  TP_ARRAYSIZE   = 8
};

enum class format_char_type : int8_t
{
  literal,   // literal char type passed through
  specifier  // timestamp format specifier
};

/**
 * @brief Represents a format specifier or literal from a timestamp format string.
 * 
 * Created by the format_compiler when parsing a format string.
 */
struct alignas(4) format_item
{
  format_char_type item_type;    // specifier or literal indicator
  char value;                    // specifier or literal value
  int8_t length;                 // item length in bytes

  static format_item new_specifier(char format_char, int8_t length)
  {
    return format_item{format_char_type::specifier,format_char,length};
  }
  static format_item new_delimiter(char literal)
  {
    return format_item{format_char_type::literal,literal,1};
  }
};

/**
 * @brief The format_compiler parses a timestamp format string into a vector of
 * format_items.
 * 
 * The vector of format_items are used when parsing a string into timestamp
 * components and when formatting a string from timestamp components.
 */
struct format_compiler
{
  static auto compile( std::string const& format, timestamp_units units) -> std::pair<std::string, std::vector<format_item>>
  {
    static std::map<char,int8_t> specifiers = {
      {'a',0}, {'A',0},
      {'w',1},
      {'b',0}, {'B',0},
      {'Y',4},{'y',2}, {'m',2}, {'d',2},
      {'H',2},{'I',2},{'M',2},{'S',2},{'f',6},
      {'p',2},{'z',5},
      {'j',3},{'U',2},{'W',2}
    };

    std::string template_string;
    std::vector<format_item> items;
    const char* str = format.c_str();
    auto length = format.length();
    while( length > 0 )
    {
      char ch = *str++;
      length--;
      if( ch!='%' )
      {
        items.push_back(format_item::new_delimiter(ch));
        template_string.append(1,ch);
        continue;
      }
      RAL_EXPECTS( length>0, "Unfinished specifier in timestamp format" );

      ch = *str++;
      length--;
      if( ch=='%' )  // escaped % char
      {
        items.push_back(format_item::new_delimiter(ch));
        template_string.append(1,ch);
        continue;
      }
      if( specifiers.find(ch)==specifiers.end() )
      {
        RAL_FAIL( "Invalid specifier" ); // show ch in here somehow
      }

      int8_t spec_length = specifiers[ch];
      if( ch=='f' )
      {
        // adjust spec_length based on units (default is 6 for micro-seconds)
        if( units==timestamp_units::ms )
          spec_length = 3;
        else if( units==timestamp_units::ns )
          spec_length = 9;
      }
      items.push_back(format_item::new_specifier(ch,spec_length));
      template_string.append((size_t)spec_length,ch);
    }

    return {template_string, items};
  }
};


// this parses date/time characters into a timestamp integer
template <typename T>  // timestamp type
struct parse_datetime
{
  std::vector<format_item> items;
  timestamp_units units;

  parse_datetime( std::vector<format_item> const& items, timestamp_units units)
    : items(items), units(units)
  {
  }

  //
  int32_t str2int( char const* str, cudf::size_type bytes )
  {
    char const* ptr = str;
    int32_t value = 0;
    for( cudf::size_type idx=0; idx < bytes; ++idx )
    {
      char chr = *ptr++;
      if( chr < '0' || chr > '9' )
        break;
      value = (value * 10) + static_cast<int32_t>(chr - '0');
    }
    return value;
  }

  // Walk the format_items to read the datetime string.
  void parse_into_parts( std::string const& str, int32_t* timeparts )
  {
    char const* ptr = str.c_str();
    cudf::size_type length = static_cast<cudf::size_type>(str.length());
    for( size_t idx=0; idx < items.size(); ++idx )
    {
      auto item = items[idx];
      if(item.item_type==format_char_type::literal)
      { // static character we'll just skip;
        // consume item.length bytes from string
        ptr += item.length;
        length -= item.length;
        continue;
      }
      if( length < item.length )
        RAL_FAIL("format string cannot be parsed from datetime string");

      // special logic for each specifier
      switch(item.value)
      {
        case 'Y':
          timeparts[TP_YEAR] = str2int(ptr,item.length);
          break;
        case 'y':
          timeparts[TP_YEAR] = str2int(ptr,item.length)+1900;
          break;
        case 'm':
          timeparts[TP_MONTH] = str2int(ptr,item.length);
          break;
        case 'd':
        case 'j':
          timeparts[TP_DAY] = str2int(ptr,item.length);
          break;
        case 'H':
        case 'I':
          timeparts[TP_HOUR] = str2int(ptr,item.length);
          break;
        case 'M':
          timeparts[TP_MINUTE] = str2int(ptr,item.length);
          break;
        case 'S':
          timeparts[TP_SECOND] = str2int(ptr,item.length);
          break;
        case 'f':
          timeparts[TP_SUBSECOND] = str2int(ptr,item.length);
          break;
        case 'p':
        {
          std::string am_pm(ptr, 2);
          if( (timeparts[TP_HOUR] <= 12) && (am_pm == "PM" || am_pm == "pm") )
            timeparts[TP_HOUR] += 12;
          break;
        }
        case 'z':
        {
          int sign = *ptr=='-' ? -1:1;
          int hh = str2int(ptr+1,2);
          int mm = str2int(ptr+3,2);
          // ignoring the rest for now
          // item.length has how many chars we should read
          timeparts[TP_TZ_MINUTES] = sign * ((hh*60)+mm);
          break;
        }
        default:
          RAL_FAIL( "Invalid specifier" );
      }
      ptr += item.length;
      length -= item.length;
    }
  }

  int64_t timestamp_from_parts( int32_t* timeparts, timestamp_units units )
  {
    auto year = timeparts[TP_YEAR];
    if( units==timestamp_units::years )
      return year - 1970;
    auto month = timeparts[TP_MONTH];
    if( units==timestamp_units::months )
      return ((year-1970) * 12) + (month-1); // months are 1-12, need to 0-base it here
    auto day = timeparts[TP_DAY];
    // The months are shifted so that March is the starting month and February
    // (possible leap day in it) is the last month for the linear calculation
    year -= (month <= 2) ? 1 : 0;
    // date cycle repeats every 400 years (era)
    constexpr int32_t erasInDays = 146097;
    constexpr int32_t erasInYears = (erasInDays / 365);
    auto era = (year >= 0 ? year : year - 399) / erasInYears;
    auto yoe = year - era * erasInYears;
    auto doy = month==0 ? day : ((153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1);
    auto doe = (yoe * 365) + (yoe / 4) - (yoe / 100) + doy;
    int32_t days = (era * erasInDays) + doe - 719468; // 719468 = days from 0000-00-00 to 1970-03-01
    if( units==timestamp_units::days )
      return days;

    auto tzadjust = timeparts[TP_TZ_MINUTES]; // in minutes
    auto hour = timeparts[TP_HOUR];
    if( units==timestamp_units::hours )
      return (days*24L) + hour + (tzadjust/60);

    auto minute = timeparts[TP_MINUTE];
    if( units==timestamp_units::minutes )
      return static_cast<int64_t>(days * 24L * 60L) + (hour * 60L) + minute + tzadjust;

    auto second = timeparts[TP_SECOND];
    int64_t timestamp = (days * 24L * 3600L) + (hour * 3600L) + (minute * 60L) + second + (tzadjust*60);
    if( units==timestamp_units::seconds )
      return timestamp;

    auto subsecond = timeparts[TP_SUBSECOND];
    if( units==timestamp_units::ms )
      timestamp *= 1000L;
    else if( units==timestamp_units::us )
      timestamp *= 1000000L;
    else if( units==timestamp_units::ns )
      timestamp *= 1000000000L;
    timestamp += subsecond;
    return timestamp;
  }

  T parse( std::string const& str)
  {
    if( str.empty() )
      return 0;
    
    int32_t timeparts[TP_ARRAYSIZE] = {0,1,1}; // month and day are 1-based
    parse_into_parts(str, timeparts);
    
    return static_cast<T>(timestamp_from_parts(timeparts,units));
  }
};

// convert cudf type to timestamp units
struct dispatch_timestamp_to_units_fn
{
  template <typename T>
  timestamp_units operator()()
  {
    RAL_FAIL("Invalid type for timestamp conversion.");
  }
};

template<>
timestamp_units dispatch_timestamp_to_units_fn::operator()<cudf::timestamp_D>() { return timestamp_units::days; }
template<>
timestamp_units dispatch_timestamp_to_units_fn::operator()<cudf::timestamp_s>() { return timestamp_units::seconds; }
template<>
timestamp_units dispatch_timestamp_to_units_fn::operator()<cudf::timestamp_ms>() { return timestamp_units::ms; }
template<>
timestamp_units dispatch_timestamp_to_units_fn::operator()<cudf::timestamp_us>() { return timestamp_units::us; }
template<>
timestamp_units dispatch_timestamp_to_units_fn::operator()<cudf::timestamp_ns>() { return timestamp_units::ns; }

// dispatch operator to map timestamp to native fixed-width-type
struct dispatch_to_timestamps_fn
{
  template <typename T, std::enable_if_t<cudf::is_timestamp<T>()>* = nullptr>
  void operator()( std::string const& str,
                    std::string const& format,
                    timestamp_units units,
                    cudf::scalar& result ) const
  {
    RAL_EXPECTS( cudf::is_timestamp<T>(), "Expecting timestamp type" );
    
    std::string template_string;
    std::vector<format_item> items;
    std::tie(template_string, items) = format_compiler::compile(format, units);
    
    parse_datetime<T> pfn{items, units};

    using ScalarType = cudf::experimental::scalar_type_t<T>;
    static_cast<ScalarType *>(&result)->set_value(pfn.parse(str));
  }

  template <typename T, std::enable_if_t<not cudf::is_timestamp<T>()>* = nullptr>
  void operator()( std::string const&,
                    std::string const&,
                    timestamp_units,
                    cudf::scalar& ) const
  {
    RAL_FAIL("Only timestamps type are expected");
  }
};

} // namespace

//
std::unique_ptr<cudf::scalar> str_to_timestamp_scalar( std::string const& str,
                                                      cudf::data_type timestamp_type,
                                                      std::string const& format )
{
  RAL_EXPECTS( !format.empty(), "Format parameter must not be empty.");
  timestamp_units units = cudf::experimental::type_dispatcher( timestamp_type, dispatch_timestamp_to_units_fn{} );

  auto result = cudf::make_timestamp_scalar(timestamp_type);
  cudf::experimental::type_dispatcher( timestamp_type, dispatch_to_timestamps_fn{},
                                      str, format, units,
                                      *result );
  return result;
}

} // namespace detail

// external API

std::unique_ptr<cudf::scalar> str_to_timestamp_scalar( std::string const& str,
                                                      cudf::data_type timestamp_type,
                                                      std::string const& format )
{
  return detail::str_to_timestamp_scalar( str, timestamp_type, format );
}

} // namespace strings
