package com.blazingdb.calcite.catalog.domain;

public enum CatalogColumnDataType {

	//TODO: handle situations where our column type is timestamp of not the default millisecond resolution
		GDF_invalid,
		GDF_INT8,
	    GDF_INT16,
	    GDF_INT32,
	    GDF_INT64,
	    GDF_FLOAT32,
		GDF_FLOAT64,
		GDF_BOOL8,
	    GDF_DATE32,   	/**< int32_t days since the UNIX epoch */
	    GDF_DATE64,   	/**< int64_t milliseconds since the UNIX epoch */
	    GDF_TIMESTAMP,	/**< Exact timestamp encoded with int64 since UNIX epoch (Default unit millisecond) */
	    GDF_CATEGORY,
	    GDF_STRING,
	    GDF_STRING_CATEGORY;

	   public static CatalogColumnDataType fromString(String type) {
			CatalogColumnDataType dataType = null;
		   switch(type) {
			case "GDF_invalid":
				dataType = GDF_invalid;
				break;
			case "GDF_INT8":
				dataType = GDF_INT8;
				break;
			case "GDF_INT16":
				dataType = GDF_INT16;
				break;
			case "GDF_INT32":
				dataType = GDF_INT32;
				break;
			case "GDF_INT64":
				dataType = GDF_INT64;
				break;
			case "GDF_FLOAT32":
				dataType = GDF_FLOAT32;
				break;
			case "GDF_FLOAT64":
				dataType = GDF_FLOAT64;
				break;
			case "GDF_BOOL8":
				dataType = GDF_BOOL8;
				break;
			case "GDF_DATE32":
				dataType = GDF_DATE32;
				break;
			case "GDF_DATE64":
				dataType = GDF_DATE64;
				break;
			case "GDF_TIMESTAMP":
				dataType = GDF_TIMESTAMP;
				break;
			case "GDF_CATEGORY":
				dataType = GDF_CATEGORY;
				break;
			case "GDF_STRING":
				dataType = GDF_STRING;
				break;
			case "GDF_STRING_CATEGORY":
				dataType = GDF_STRING_CATEGORY;
				break;
			default:
				dataType = null;
			}
		   return dataType;
		}
}
