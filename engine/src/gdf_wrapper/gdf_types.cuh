#ifndef _BZ_OPS_H_
#define _BZ_OPS_H_




typedef enum{
	BLZ_FLOOR,
	BLZ_CEIL,
	BLZ_SIN,
	BLZ_COS,
	BLZ_ASIN,
	BLZ_ACOS,
	BLZ_TAN,
	BLZ_COTAN,
	BLZ_ATAN,
	BLZ_ABS,
	BLZ_NOT,
	BLZ_LN,
	BLZ_LOG,
	BLZ_YEAR,
	BLZ_MONTH,
	BLZ_DAY,
	BLZ_HOUR,
	BLZ_MINUTE,
	BLZ_SECOND,
	BLZ_IS_NULL,
	BLZ_IS_NOT_NULL,
	BLZ_CAST_INTEGER,
	BLZ_CAST_BIGINT,
	BLZ_CAST_FLOAT,
	BLZ_CAST_DOUBLE,
	BLZ_CAST_DATE,
	BLZ_CAST_TIMESTAMP,
	BLZ_CAST_VARCHAR,
	BLZ_INVALID_UNARY

} gdf_unary_operator;


typedef enum {
  BLZ_ADD,            ///< operator +
  BLZ_SUB,            ///< operator -
  BLZ_MUL,            ///< operator *
  BLZ_DIV,            ///< operator / using common type of lhs and rhs
  BLZ_MOD,            ///< operator %
  BLZ_POW,            ///< lhs ^ rhs
  BLZ_EQUAL,          ///< operator ==
  BLZ_NOT_EQUAL,      ///< operator !=
  BLZ_LESS,           ///< operator <
  BLZ_GREATER,        ///< operator >
  BLZ_LESS_EQUAL,     ///< operator <=
  BLZ_GREATER_EQUAL,  ///< operator >=
  BLZ_BITWISE_AND,    ///< operator &
  BLZ_BITWISE_OR,     ///< operator |
  BLZ_BITWISE_XOR,    ///< operator ^
  BLZ_LOGICAL_AND,    ///< operator &&
  BLZ_LOGICAL_OR,     ///< operator ||
	BLZ_FIRST_NON_MAGIC,
	BLZ_MAGIC_IF_NOT,
	BLZ_STR_LIKE,
	BLZ_STR_SUBSTRING,
	BLZ_STR_CONCAT,
  BLZ_INVALID_BINARY  ///< invalid operation
} gdf_binary_operator_exp;


#endif /* _BZ_OPS_H_ */
