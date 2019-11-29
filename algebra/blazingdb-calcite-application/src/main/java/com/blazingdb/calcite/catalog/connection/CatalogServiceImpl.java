package com.blazingdb.calcite.catalog.connection;

import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;
import com.blazingdb.calcite.catalog.domain.CatalogSchemaImpl;
import com.blazingdb.calcite.catalog.domain.CatalogTable;
import com.blazingdb.calcite.catalog.domain.CatalogTableImpl;
import com.blazingdb.calcite.catalog.repository.DatabaseRepository;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CatalogServiceImpl {
	DatabaseRepository repo;
	Set<String> dbNames;
	public CatalogServiceImpl() {
		dbNames = new HashSet<>();
		repo = new DatabaseRepository();
	}

	public void
	dropAllTables() {
		for(String dbName : this.dbNames) {
			CatalogDatabaseImpl db = repo.getDatabase(dbName);
			for(String tableName : db.getTableNames()) {
				db.removeTable(tableName);
			}
			repo.updateDatabase(db);
		}
	}

	public CatalogSchemaImpl
	getSchema(String schemaName) {
		return null;
	}

	// for calcite schema get subschemas
	public CatalogDatabaseImpl
	getDatabase(String databaseName) {
		return repo.getDatabase(databaseName);
	}

	public Collection<CatalogTable>
	getTables(String databaseName) {
		return null;
	}

	public CatalogTableImpl
	getTable(String schemaName, String tableName) {
		return null;
	}

	// TODO we may not need this api
	public CatalogTableImpl
	getTable(String tableName) {
		return null;
	}

	public void
	createDatabase(CatalogDatabaseImpl db) {
		repo.createDatabase(db);
		dbNames.add(db.getDatabaseName());
	}
}
