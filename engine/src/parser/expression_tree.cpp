#include "expression_tree.hpp"
#include <cassert>
#include <limits.h>

namespace ral {
namespace parser {
namespace detail {

constexpr char lexer::VARIABLE_REGEX_STR[];
constexpr char lexer::NULL_REGEX_STR[];
constexpr char lexer::BOOLEAN_REGEX_STR[];
constexpr char lexer::NUMBER_REGEX_STR[];
constexpr char lexer::TIMESTAMP_D_REGEX_STR[];
constexpr char lexer::TIMESTAMP_S_REGEX_STR[];
constexpr char lexer::TIMESTAMP_MS_REGEX_STR[];
constexpr char lexer::TIMESTAMP_US_REGEX_STR[];
constexpr char lexer::TIMESTAMP_NS_REGEX_STR[];
constexpr char lexer::STRING_REGEX_STR[];

lexer::lexer(const std::string & str)
            : text_(str)
            , pos_{0}
{
  text_ = StringUtil::trim(text_);
}

void lexer::advance(size_t offset) { pos_ += offset; }

lexer::token lexer::next_token() {
  if (pos_ >= text_.size()) {
    return {lexer::token_type::EOF_, ""};
  }

  // Discard whitespaces
  while (text_[pos_] == ' ') advance();

  if (text_[pos_] == '(') {
    advance();
    assert(pos_ <= text_.length());
    return {lexer::token_type::ParenthesisOpen, "("};
  }

  if (text_[pos_] == ')') {
    advance();
    assert(pos_ <= text_.length());
    return {lexer::token_type::ParenthesisClose, ")"};
  }

  if (text_[pos_] == ',') {
    advance();
    assert(pos_ <= text_.length());
    return {lexer::token_type::Comma, ","};
  }

  if (text_[pos_] == ':') {
    advance();
    assert(pos_ <= text_.length());
    return {lexer::token_type::Colon, ":"};
  }

  std::smatch match;
  std::string remainder = text_.substr(pos_);
  if (std::regex_search(remainder, match, variable_regex)) {
    advance(match.length());
    assert(pos_ <= text_.length());
    return {lexer::token_type::Variable, match.str()};
  }

  if (std::regex_search(remainder, match, null_regex)) {
    advance(match.length());
    assert(pos_ <= text_.length());
    return {lexer::token_type::Null, match.str()};
  }

  if (std::regex_search(remainder, match, boolean_regex)) {
    advance(match.length());
    assert(pos_ <= text_.length());
    return {lexer::token_type::Boolean, match.str()};
  }

  if (std::regex_search(remainder, match, timestamp_ns_regex)) {
    advance(match.length());
    assert(pos_ <= text_.length());
    return {lexer::token_type::Timestamp_ns, match.str()};
  }

  if (std::regex_search(remainder, match, timestamp_us_regex)) {
    advance(match.length());
    assert(pos_ <= text_.length());
    return {lexer::token_type::Timestamp_us, match.str()};
  }

  if (std::regex_search(remainder, match, timestamp_ms_regex)) {
    advance(match.length());
    assert(pos_ <= text_.length());
    return {lexer::token_type::Timestamp_ms, match.str()};
  }

  if (std::regex_search(remainder, match, timestamp_s_regex)) {
    advance(match.length());
    assert(pos_ <= text_.length());
    return {lexer::token_type::Timestamp_s, match.str()};
  }

  if (std::regex_search(remainder, match, timestamp_d_regex)) {
    advance(match.length());
    assert(pos_ <= text_.length());
    return {lexer::token_type::Timestamp_d, match.str()};
  }

  if (std::regex_search(remainder, match, number_regex)) {
    advance(match.length());
    assert(pos_ <= text_.length());
    return {lexer::token_type::Number, match.str()};
  }

  if (std::regex_search(remainder, match, string_regex)) {
    advance(match.length());
    assert(pos_ <= text_.length());
    return {lexer::token_type::String, match.str()};
  }

  size_t len = 0;
  char ch;
  do {
    ch = text_[pos_ + len];

    if (ch == '(' || ch == ')' || ch == ',' || ch == ':')
        break;
  } while ((pos_ + (++len)) < text_.length());

  std::string value = text_.substr(pos_, len);

  advance(len);

  return {lexer::token_type::Identifier, value};
}

expr_parser::expr_parser(const std::string & expr_str) : lexer_{expr_str} {}

std::unique_ptr<node> expr_parser::parse() {
  token_ = lexer_.next_token();

  return expr();
}

bool expr_parser::accept(lexer::token_type type) {
  if (token_.type == type) {
    token_ = lexer_.next_token();
    return true;
  }

  return false;
}

std::unique_ptr<node> expr_parser::expr(){
  // expr    : term | func
  // func    : indentifier '(' funcArgs? ')' (':' indentifier)?
  // funcArgs: expr(',' expr)*
  // term    : variable | literal
  // literal : (null | boolean | number | timestamp | string)(':' indentifier)?

  auto ret = term();
  if (!ret) {
      ret = func();
  }

  RAL_EXPECTS(!!ret, "Couldn't parse calcite expression");

  return ret;
}

std::unique_ptr<node> expr_parser::term() {
  lexer::token variable_token = token_;
  if (accept(lexer::token_type::Variable)) {
      return std::unique_ptr<node>(new variable_node(variable_token.value));
  }

  return literal();
}

std::unique_ptr<node> expr_parser::func() {
  lexer::token func_name_token = token_;
  if(accept(lexer::token_type::Identifier)){
    accept(lexer::token_type::ParenthesisOpen);

    std::vector<std::unique_ptr<node>> args;
    if (!accept(lexer::token_type::ParenthesisClose)) {
      args = std::move(func_args());
      accept(lexer::token_type::ParenthesisClose);
    }

    std::string func_identifier = func_name_token.value;
    if (accept(lexer::token_type::Colon)) {
      lexer::token return_token = token_;
      accept(lexer::token_type::Identifier);

      // Just append the return type to the function name for now
      // Example: CAST():INTEGER => CAST_INTEGER()
      func_identifier += "_" + return_token.value;
    }

    auto ret = std::unique_ptr<node>(new operator_node(func_identifier));
    ret->children = std::move(args);

    return std::move(ret);
  }

  return nullptr;
}

std::vector<std::unique_ptr<node>> expr_parser::func_args() {
  std::vector<std::unique_ptr<node>> ret;
  ret.push_back(expr());

  while (accept(lexer::token_type::Comma)) {
    ret.push_back(expr());
  }

  return std::move(ret);
}

std::unique_ptr<node> expr_parser::literal() {
  lexer::token literal_token = token_;
  if (accept(lexer::token_type::Null)
      || accept(lexer::token_type::Boolean)
      || accept(lexer::token_type::Number)
      || accept(lexer::token_type::Timestamp_d)
      || accept(lexer::token_type::Timestamp_s)
      || accept(lexer::token_type::Timestamp_ms)
      || accept(lexer::token_type::Timestamp_us)
      || accept(lexer::token_type::Timestamp_ns)
      || accept(lexer::token_type::String))
  {
    cudf::data_type type;
    if (accept(lexer::token_type::Colon)) {
        lexer::token type_token = token_;
        accept(lexer::token_type::Identifier);
        type = type_from_type_token(type_token);
    } else {
      type = infer_type_from_literal_token(literal_token);
    }

    return std::unique_ptr<node>(new literal_node(literal_token.value, type));
  }

  return nullptr;
}

cudf::data_type infer_type_from_literal_token(const lexer::token & token) {
  if(token.type == lexer::token_type::Null) {
    return cudf::data_type{cudf::type_id::EMPTY};
  } else if(token.type == lexer::token_type::Boolean) {
    return cudf::data_type{cudf::type_id::BOOL8};
  } else if(token.type == lexer::token_type::Number) {
    const std::string & token_value = token.value;
    if(token_value.find_first_of(".eE") != std::string::npos) {
      double parsed_double = std::stod(token_value);
      float casted_float = static_cast<float>(parsed_double);
      return parsed_double == casted_float ? cudf::data_type{cudf::type_id::FLOAT32} : cudf::data_type{cudf::type_id::FLOAT64};
    } else {
      int64_t parsed_int64 = std::stoll(token_value);
      if (parsed_int64 > INT_MAX){
        return cudf::data_type{cudf::type_id::INT64};
      } else {
        // as other SQL engines, defaults to int32
        return cudf::data_type{cudf::type_id::INT32};
      }      
    }
  } else if (token.type == lexer::token_type::Timestamp_ns) {
    return cudf::data_type{cudf::type_id::TIMESTAMP_NANOSECONDS};
  } else if (token.type == lexer::token_type::Timestamp_us) {
    return cudf::data_type{cudf::type_id::TIMESTAMP_MICROSECONDS};
  } else if (token.type == lexer::token_type::Timestamp_ms) {
    return cudf::data_type{cudf::type_id::TIMESTAMP_MILLISECONDS};
  } else if (token.type == lexer::token_type::Timestamp_s) {
    return cudf::data_type{cudf::type_id::TIMESTAMP_SECONDS};
  } else if(token.type == lexer::token_type::Timestamp_d) {
    return cudf::data_type{cudf::type_id::TIMESTAMP_DAYS};
  } else { // token.type == lexer::token_type::String
    return cudf::data_type{cudf::type_id::STRING};
  }
}

cudf::data_type type_from_type_token(const lexer::token & token) {
  const std::string & token_value = token.value;
  if (token_value == "NULL" || token_value == "BOOLEAN") {
    // Default Null type to boolean
    return cudf::data_type{cudf::type_id::BOOL8};
  }
  if (token_value == "TINYINT") {
    return cudf::data_type{cudf::type_id::INT8};
  }
  if (token_value == "SMALLINT") {
    return cudf::data_type{cudf::type_id::INT16};
  }

  if (token_value == "INTEGER"
      //INTERVALS MONTH AND YEAR ARE NOT CURRENTLY SUPPORTED
      || token_value == "INTERVAL SECOND"
      || token_value == "INTERVAL MINUTE"
      || token_value == "INTERVAL HOUR"
      || token_value == "INTERVAL DAY" ) {
    return cudf::data_type{cudf::type_id::INT32};
  }
  if (token_value == "BIGINT") {
    return cudf::data_type{cudf::type_id::INT64};
  }
  if (token_value == "FLOAT") {
    return cudf::data_type{cudf::type_id::FLOAT32};
  }
  if (token_value == "DOUBLE") {
    return cudf::data_type{cudf::type_id::FLOAT64};
  }
  if (token_value == "DATE") {
    return cudf::data_type{cudf::type_id::TIMESTAMP_DAYS};
  }
  if (token_value == "TIMESTAMP") {
    return cudf::data_type{cudf::type_id::TIMESTAMP_NANOSECONDS};
  }
  if (token_value == "VARCHAR") {
    return cudf::data_type{cudf::type_id::STRING};
  }

  RAL_FAIL("Invalid literal cast type");
}

} // namespace detail
} // namespace parser
} // namespace ral
