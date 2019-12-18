package com.blazingdb.calcite.application;

import com.blazingdb.calcite.catalog.connection.CatalogServiceImpl;
import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;
import com.blazingdb.calcite.schema.BlazingSchema;
// TODO: Hibernate should allow us to be able to update and persist things on the fly
// without having to reload everything. We had a bug with this so now are refreshing
// the schema from the data source each time ddl is run
/**
 * <h1>Stores state of the application.</h1>
 * This is the class which stores the state of the application. It has a single {@link #schema}
 * for now. It also is what has access to the {@link #catalogService} which is used for persistence
 * as well as the {@link #relationalAlgebraGenerator} for converting sql to a {@see org.apache.calcite.rel.RelNode}
 *
 *
 * @author  Felipe Aramburu
 * @version 1.0
 * @since   2018-10-31
 */
public class ApplicationContext {
	/**
	 * Handles all ddl queries. Persists this information to whatever
	 * source has been configured for hibernate.
	 */
	private CatalogServiceImpl catalogService;
	// assuming just one database for now
	/**
	 * Stores the schema with all the tables and their definitions.
	 */
	private BlazingSchema schema;
	/**
	 * Used to take sql and convert it to optimied relational algebra logical plans.
	 */
	RelationalAlgebraGenerator relationalAlgebraGenerator;
	private static ApplicationContext instance = null;

	/**
	 * Private Constructore method. This class is currently a singleton so this is never invoked
	 * outside of the class.
	 */
	private ApplicationContext() {
		catalogService = new CatalogServiceImpl();
		CatalogDatabaseImpl db = catalogService.getDatabase("main");
		if(db == null) {
			db = new CatalogDatabaseImpl("main");
			catalogService.createDatabase(db);
		}
		db = catalogService.getDatabase("main");
		schema = new BlazingSchema(db);
		relationalAlgebraGenerator = new RelationalAlgebraGenerator(schema);
	}
	/**
	 * Initializes the application context by calling the private constructor if necessary
	 * before any operations occur.
	 */
	public static void
	init() {
		if(instance == null) {
			instance = new ApplicationContext();
		}
	}
	/**
	 * Getter function for catalogService
	 * @return {@link #catalogService}
	 */
	public static CatalogServiceImpl
	getCatalogService() {
		init();
		return instance.getService();
	}
	/**
	 * Getter function for relationalAlgebraGenerator
	 * @return {@link #relationalAlgebraGenerator}
	 */
	public static RelationalAlgebraGenerator
	getRelationalAlgebraGenerator() {
		init();
		return instance.getAlgebraGenerator();
	}

	/**
	 * Synchronizes access to the catalog service in case an update is occurring.
	 *
	 * @return {@link #catalogService}
	 */
	private synchronized CatalogServiceImpl
	getService() {
		return catalogService;
	}
	/**
	 * Synchronizes access to the relational algebra generator in case an update is occurring
	 * @return {@link #relationalAlgebraGenerator}
	 */
	private synchronized RelationalAlgebraGenerator
	getAlgebraGenerator() {
		return this.relationalAlgebraGenerator;
	}

	/**
	 * Refreshes the schema after ddl has occurred. The application context needs to reload
	 * every time we update the ddl right now. Synchronizes access to avoid two threads doing this at once.
	 *
	 */
	private synchronized void
	updateSchema() {
		schema = new BlazingSchema(catalogService.getDatabase("main"));
		relationalAlgebraGenerator = new RelationalAlgebraGenerator(schema);
	}

	/**
	 * The public api for update the context. Calls the synchronized method {@link #updateSchema()}
	 */
	public static void
	updateContext() {
		// TODO Auto-generated method stub
		init();
		instance.updateSchema();
	}
}
