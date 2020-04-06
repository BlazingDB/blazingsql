/*
 * Copyright 2018 BlazingDB, Inc.
 *     Copyright 2018 Percy Camilo Triveño Aucahuasi <percy@blazingdb.com>
 */

package com.blazingdb.calcite.schema;

import com.blazingdb.calcite.catalog.domain.CatalogColumn;
import com.blazingdb.calcite.catalog.domain.CatalogColumnDataType;
import com.blazingdb.calcite.catalog.domain.CatalogTable;

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BlazingTable implements ProjectableFilterableTable {
	final static Logger LOGGER = LoggerFactory.getLogger(BlazingTable.class);

	private final CatalogTable catalogTable;

	public BlazingTable(CatalogTable catalogTable) { this.catalogTable = catalogTable; }

	@Override
	public RelDataType
	getRowType(RelDataTypeFactory rdtf) {
		RelDataTypeFactory.FieldInfoBuilder builder = rdtf.builder();
		// for (TColumnType tct : rowInfo.getRow_desc()) {
		// // MAPDLOGGER.debug("'" + tct.col_name + "'" + " \t" + tct.getCol_type().getEncoding() + " \t"
		// // + tct.getCol_type().getFieldValue(TTypeInfo._Fields.TYPE) + " \t" + tct.getCol_type().nullable
		// // + " \t" + tct.getCol_type().is_array + " \t" + tct.getCol_type().precision + " \t"
		// // + tct.getCol_type().scale);
		// //
		// // builder.add(tct.col_name, createType(tct, rdtf));
		//
		// // TODO percy
		// String col_name = null;
		// RelDataType col_type = null;
		//
		// builder.add(col_name, col_type);
		// }
		for(CatalogColumn column : catalogTable.getColumns()) {
			builder.add(column.getColumnName(), convertToSqlType(column.getColumnDataType(), rdtf));
			builder.nullable(true);
		}
		return builder.build();
	}

	@Override
	public Statistic
	getStatistic() {
		return Statistics.UNKNOWN;
	}

	@Override
	public Schema.TableType
	getJdbcTableType() {
		return Schema.TableType.TABLE;
	}

	private RelDataType
	convertToSqlType(CatalogColumnDataType dataType, RelDataTypeFactory typeFactory) {
		RelDataType temp = null;
		switch(dataType) {
			case INT8:
				temp = typeFactory.createSqlType(SqlTypeName.TINYINT);
				break;
			case INT16:
				temp = typeFactory.createSqlType(SqlTypeName.SMALLINT);
				break;
			case INT32:
				temp = typeFactory.createSqlType(SqlTypeName.INTEGER);
				break;
			case INT64:
				temp = typeFactory.createSqlType(SqlTypeName.BIGINT);
				break;
			case FLOAT32:
				temp = typeFactory.createSqlType(SqlTypeName.FLOAT);
				break;
			case FLOAT64:
				temp = typeFactory.createSqlType(SqlTypeName.DOUBLE);
				break;
			case BOOL8:
				temp = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
				break;
			case TIMESTAMP_DAYS:
			case TIMESTAMP_SECONDS:
				temp = typeFactory.createSqlType(SqlTypeName.DATE);
				break;
			case TIMESTAMP_MILLISECONDS:
			case TIMESTAMP_MICROSECONDS:
			case TIMESTAMP_NANOSECONDS:
				temp = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
				break;
			case DICTIONARY32:
			case STRING:
				temp = typeFactory.createSqlType(SqlTypeName.VARCHAR);
				break;
//			case STRING_CATEGORY:
//				temp = typeFactory.createSqlType(SqlTypeName.VARCHAR);
//				break;
			default:
				temp = null;
		}
		return temp;
	}
	// private RelDataType createType(TColumnType value, RelDataTypeFactory typeFactory) {
	// RelDataType cType = getRelDataType(value.col_type.type, value.col_type.precision, value.col_type.scale,
	// typeFactory);
	// if (value.col_type.is_array) {
	// if (value.col_type.isNullable()) {
	// return typeFactory.createArrayType(typeFactory.createTypeWithNullability(cType, true), -1);
	// } else {
	// return typeFactory.createArrayType(cType, -1);
	// }
	// } else if (value.col_type.isNullable()) {
	// return typeFactory.createTypeWithNullability(cType, true);
	// } else {
	// return cType;
	// }
	// }

	// Convert our TDataumn type in to a base calcite SqlType
	// todo confirm whether it is ok to ignore thinsg like lengths
	// since we do not use them on the validator side of the calcite 'fence'
	// private RelDataType getRelDataType(TDatumType dType, int precision, int scale, RelDataTypeFactory typeFactory) {
	// switch (dType) {
	// case SMALLINT:
	// return typeFactory.createSqlType(SqlTypeName.SMALLINT);
	// case INT:
	// return typeFactory.createSqlType(SqlTypeName.INTEGER);
	// case BIGINT:
	// return typeFactory.createSqlType(SqlTypeName.BIGINT);
	// case FLOAT:
	// return typeFactory.createSqlType(SqlTypeName.FLOAT);
	// case DECIMAL:
	// return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
	// case DOUBLE:
	// return typeFactory.createSqlType(SqlTypeName.DOUBLE);
	// case STR:
	// return typeFactory.createSqlType(SqlTypeName.VARCHAR, 50);
	// case TIME:
	// return typeFactory.createSqlType(SqlTypeName.TIME);
	// case TIMESTAMP:
	// return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
	// case DATE:
	// return typeFactory.createSqlType(SqlTypeName.DATE);
	// case BOOL:
	// return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
	// case INTERVAL_DAY_TIME:
	// return typeFactory.createSqlType(SqlTypeName.INTERVAL_DAY);
	// case INTERVAL_YEAR_MONTH:
	// return typeFactory.createSqlType(SqlTypeName.INTERVAL_YEAR_MONTH);
	// case POINT:
	// return typeFactory.createSqlType(SqlTypeName.ANY);
	// // return new PointSqlType();
	// case LINESTRING:
	// return typeFactory.createSqlType(SqlTypeName.ANY);
	// // return new LinestringSqlType();
	// case POLYGON:
	// return typeFactory.createSqlType(SqlTypeName.ANY);
	// // return new PolygonSqlType();
	// case MULTIPOLYGON:
	// return typeFactory.createSqlType(SqlTypeName.ANY);
	// default:
	// throw new AssertionError(dType.name());
	// }
	// }

	@Override
	public boolean
	isRolledUp(String string) {
		// will set to false by default
		return false;
	}

	@Override
	public boolean
	rolledUpColumnValidInsideAgg(String string, SqlCall sc, SqlNode sn, CalciteConnectionConfig ccc) {
		throw new UnsupportedOperationException("rolledUpColumnValidInsideAgg Not supported yet.");
	}


	@Override
	public Enumerable<Object[]>
	scan(DataContext root, List<RexNode> filters, int[] projects) {
		// TODO Auto-generated method stub

		return null;
	}
}
