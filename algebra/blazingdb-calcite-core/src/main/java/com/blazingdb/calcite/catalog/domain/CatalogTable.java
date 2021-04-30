package com.blazingdb.calcite.catalog.domain;

import java.util.Set;

public interface CatalogTable {
	public String
	getTableName();

	public Set<CatalogColumn>
	getColumns();

	public CatalogDatabase
	getDatabase();

	public Double
	getRowCount();
}
