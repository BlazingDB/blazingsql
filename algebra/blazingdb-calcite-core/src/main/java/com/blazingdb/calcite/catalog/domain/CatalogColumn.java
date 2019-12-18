package com.blazingdb.calcite.catalog.domain;

public interface CatalogColumn {
	public String
	getColumnName();

	public CatalogColumnDataType
	getColumnDataType();

	public CatalogTable
	getTable();
}
