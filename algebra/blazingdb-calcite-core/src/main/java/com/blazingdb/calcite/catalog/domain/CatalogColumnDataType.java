package com.blazingdb.calcite.catalog.domain;

//// TODO: handle situations where our column type is timestamp of not the default millisecond resolution
//GDF_invalid,
//GDF_INT8,
//GDF_INT16,
//GDF_INT32,
//GDF_INT64,
//GDF_FLOAT32,
//GDF_FLOAT64,
//GDF_BOOL8,
//GDF_DATE32,	/**< int32_t days since the UNIX epoch */
//GDF_DATE64,	/**< int64_t milliseconds since the UNIX epoch */
//GDF_TIMESTAMP, /**< Exact timestamp encoded with int64 since UNIX epoch (Default unit millisecond) */
//GDF_CATEGORY,
//GDF_STRING,
//GDF_STRING_CATEGORY;

public enum CatalogColumnDataType {
	// See cudf/types.hpp type_id enum
	EMPTY(0, "EMPTY"), /// < Always null with no underlying data
	INT8(1, "INT8"), /// < 1 byte signed integer
	INT16(2, "INT16"), /// < 2 byte signed integer
	INT32(3, "INT32"), /// < 4 byte signed integer
	INT64(4, "INT64"), /// < 8 byte signed integer
	FLOAT32(5, "FLOAT32"), /// < 4 byte floating point
	FLOAT64(6, "FLOAT64"), /// < 8 byte floating point
	BOOL8(7, "BOOL8"), /// < Boolean using one byte per value, 0 == false, else true
	TIMESTAMP_DAYS(8, "TIMESTAMP_DAYS"), /// < days since Unix Epoch in int32
	TIMESTAMP_SECONDS(9, "TIMESTAMP_SECONDS"), /// < duration of seconds since Unix Epoch in int64
	TIMESTAMP_MILLISECONDS(10, "TIMESTAMP_MILLISECONDS"), /// < duration of milliseconds since Unix Epoch in int64
	TIMESTAMP_MICROSECONDS(11, "TIMESTAMP_MICROSECONDS"), /// < duration of microseconds since Unix Epoch in int64
	TIMESTAMP_NANOSECONDS(12, "TIMESTAMP_NANOSECONDS"), /// < duration of nanoseconds since Unix Epoch in int64
	DICTIONARY32(13, "DICTIONARY32"),
	STRING(14, "STRING"), /// < String elements
	// `NUM_TYPE_IDS` must be last!
	NUM_TYPE_IDS(15, "NUM_TYPE_IDS"); /// < Total number of type ids

	private final int type_id;
	private final String type_id_name;

	private CatalogColumnDataType(int type_id, String type_id_name) {
		this.type_id = type_id;
		this.type_id_name = type_id_name;
	}

	public final int getTypeId() {
		return this.type_id;
	}

	public final String getTypeIdName() {
		return this.type_id_name;
	}

	public static CatalogColumnDataType fromTypeId(int type_id) {
		for (CatalogColumnDataType verbosity : CatalogColumnDataType.values()) {
			if (verbosity.getTypeId() == type_id)
				return verbosity;
		}

		return EMPTY;
	}

	public static CatalogColumnDataType fromString(final String type_id_name) {
		CatalogColumnDataType dataType = null;
		switch (type_id_name) {
			case "EMPTY": return EMPTY;
			case "INT8": return INT8;
			case "INT16": return INT16;
			case "INT32": return INT32;
			case "INT64": return INT64;
			case "FLOAT32": return FLOAT32;
			case "FLOAT64": return FLOAT64;
			case "BOOL8": return BOOL8;
			case "TIMESTAMP_DAYS": return TIMESTAMP_DAYS;
			case "TIMESTAMP_SECONDS": return TIMESTAMP_SECONDS;
			case "TIMESTAMP_MILLISECONDS": return TIMESTAMP_MILLISECONDS;
			case "TIMESTAMP_MICROSECONDS": return TIMESTAMP_MICROSECONDS;
			case "TIMESTAMP_NANOSECONDS": return TIMESTAMP_NANOSECONDS;
			case "DICTIONARY32": return DICTIONARY32;
			case "STRING": return STRING;
			case "NUM_TYPE_IDS": return NUM_TYPE_IDS;
		}
		return dataType;
	}
}
