#ifndef BLAZINGDB_UC_UTIL_MACROS_HPP_
#define BLAZINGDB_UC_UTIL_MACROS_HPP_

#ifdef __GNUC__
#define UC_INLINE inline __attribute__((always_inline))
#else
#define UC_INLINE inline
#endif

#define BZ_INTERFACE(Kind)               \
public:                                  \
  virtual ~Kind() = default;             \
                                         \
protected:                               \
  explicit Kind() = default;             \
                                         \
private:                                 \
  Kind(const Kind &) = delete;           \
  Kind(const Kind &&) = delete;          \
  void operator=(const Kind &) = delete; \
  void operator=(const Kind &&) = delete

#define DefineClassName(className) \
  static std::string MessageID() { return #className; }

#endif
