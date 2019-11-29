package com.blazingdb.calcite.catalog.connection;

import java.util.Collection;

import com.blazingdb.calcite.catalog.domain.CatalogDatabase;
import com.blazingdb.calcite.catalog.domain.CatalogSchema;
import com.blazingdb.calcite.catalog.domain.CatalogTable;

public interface CatalogService {

	public CatalogSchema getSchema(String schemaName);

	// for calcite schema get subschemas
	public CatalogDatabase getDatabase(String schemaName, String databaseName);

	public Collection<CatalogTable> getTables(String databaseName);

	public CatalogTable getTable(String schemaName, String tableName);

	// TODO we may not need this api
	public CatalogTable getTable(String tableName);

}
