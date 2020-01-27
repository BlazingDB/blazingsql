package com.blazingdb.calcite.catalog;

import com.blazingdb.calcite.application.CalciteApplication;
import com.blazingdb.calcite.application.RelationalAlgebraGenerator;
import com.blazingdb.calcite.catalog.domain.CatalogColumnDataType;
import com.blazingdb.calcite.catalog.domain.CatalogColumnImpl;
import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;
import com.blazingdb.calcite.catalog.domain.CatalogTable;
import com.blazingdb.calcite.catalog.domain.CatalogTableImpl;
import com.blazingdb.calcite.catalog.repository.DatabaseRepository;
import com.blazingdb.calcite.schema.BlazingSchema;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.commons.dbcp2.BasicDataSource;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import java.io.IOException;
import javax.naming.NamingException;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.FileSystemResourceAccessor;
import liquibase.resource.ResourceAccessor;

public class BlazingCatalogTest {
	private static SessionFactory sessionFactory = null;

	private static final String LIQUIBASE_CHANGELOG = "liquibase.changelog";
	private static final String LIQUIBASE_DATASOURCE = "liquibase.datasource";


	private String dataSourceName;
	private String changeLogFile;
	private String contexts;
	private String labels;

	private void
	executeUpdate() throws NamingException, SQLException, LiquibaseException, InstantiationException,
						   IllegalAccessException, ClassNotFoundException {
		// setDataSource((String) servletValueContainer.getValue(LIQUIBASE_DATASOURCE));

		this.dataSourceName = "bz3";

		if(this.dataSourceName == null) {
			throw new RuntimeException("Cannot run Liquibase, " + LIQUIBASE_DATASOURCE + " is not set");
		}

		// setChangeLogFile((String) servletValueContainer.getValue(LIQUIBASE_CHANGELOG));

		String changeLogFile = "liquibase-bz-master.xml";
		this.changeLogFile = changeLogFile;

		if(this.changeLogFile == null) {
			throw new RuntimeException("Cannot run Liquibase, " + LIQUIBASE_CHANGELOG + " is not set");
		}

		// setContexts((String) servletValueContainer.getValue(LIQUIBASE_CONTEXTS));
		this.contexts = "";
		// setLabels((String) servletValueContainer.getValue(LIQUIBASE_LABELS));

		this.labels = "";

		// this.defaultSchema = StringUtil.trimToNull((String)
		// servletValueContainer.getValue(LIQUIBASE_SCHEMA_DEFAULT));
		// this.defaultSchema =

		Connection connection = null;
		Database database = null;
		try {
			// DriverManager.registerDriver((Driver) Class.forName("com.mysql.jdbc.Driver").newInstance());
			// String url = "jdbc:mysql://localhost:3306/bz3";
			// connection = DriverManager.getConnection(url);

			BasicDataSource dataSource = new BasicDataSource();
			dataSource.setDriverClassName("com.mysql.jdbc.Driver");
			dataSource.setUsername("blazing");
			dataSource.setPassword("blazing");
			dataSource.setUrl("jdbc:mysql://localhost:3306/bz3");
			dataSource.setMaxTotal(10);
			dataSource.setMaxIdle(5);
			dataSource.setInitialSize(5);
			dataSource.setValidationQuery("SELECT 1");

			// MySQLData dataSource = new JdbcDataSource(); // (DataSource) ic.lookup(this.dataSourceName);
			// dataSource.setURL("jdbc:mysql://localhost:3306/bz3");
			// dataSource.setUser("blazing");
			// dataSource.setPassword("blazing");
			connection = dataSource.getConnection();

			Thread currentThread = Thread.currentThread();
			ClassLoader contextClassLoader = currentThread.getContextClassLoader();
			ResourceAccessor threadClFO = new ClassLoaderResourceAccessor(contextClassLoader);

			ResourceAccessor clFO = new ClassLoaderResourceAccessor();
			ResourceAccessor fsFO = new FileSystemResourceAccessor();

			database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
			database.setDefaultSchemaName(this.dataSourceName);
			Liquibase liquibase =
				new Liquibase(this.changeLogFile, new CompositeResourceAccessor(clFO, fsFO, threadClFO), database);

			// @SuppressWarnings("unchecked")
			// StringTokenizer initParameters = new StringTokenizer(""); // servletContext.getInitParameterNames();
			// while (initParameters.hasMoreElements()) {
			// String name = initParameters.nextElement().trim();
			// if (name.startsWith(LIQUIBASE_PARAMETER + ".")) {
			// // liquibase.setChangeLogParameter(name.substring(LIQUIBASE_PARAMETER.length() + 1),
			// // servletValueContainer.getValue(name));
			// }
			// }

			liquibase.update(new Contexts(this.contexts), new LabelExpression(this.labels));
		} finally {
			if(database != null) {
				database.close();
			} else if(connection != null) {
				connection.close();
			}
		}
	}

	@BeforeMethod
	public void
	setUp() throws Exception {
		// this.executeUpdate();
		CalciteApplication.executeUpdate();
		repo = new DatabaseRepository();
		sessionFactory = new Configuration().configure().buildSessionFactory();
	}
	DatabaseRepository repo;
	@AfterMethod
	public void
	tearDown() throws Exception {
		sessionFactory.close();
	}
	private Long dbId = -1L;
	@Test()
	public void
	createDatabaseTest() throws Exception {
		System.out.println("=============================== CREATE DATABASE TEST ====================================");

		CatalogDatabaseImpl db = new CatalogDatabaseImpl("db-test");

		repo.createDatabase(db);
		dbId = db.getId();

		db = repo.getDatabase(dbId);

		// commment out the line below to allow things to stay
		repo.dropDatabase(db);
	}


	@Test()
	public void
	createTableTest() throws Exception {
		System.out.println("=============================== CREATE TABLE TEST ====================================");

		CatalogDatabaseImpl db = new CatalogDatabaseImpl("table-test-db");

		repo.createDatabase(db);
		dbId = db.getId();

		CatalogColumnImpl column1 = new CatalogColumnImpl("col-1", CatalogColumnDataType.INT64, 1);
		CatalogColumnImpl column2 = new CatalogColumnImpl("col-2", CatalogColumnDataType.INT32, 2);

		List<CatalogColumnImpl> columns = new ArrayList<CatalogColumnImpl>();
		columns.add(column1);
		columns.add(column2);

		CatalogTableImpl table = new CatalogTableImpl("table-1", db, columns);

		// repo.createTable(table);
		db.addTable(table);
		repo.updateDatabase(db);

		db = repo.getDatabase(dbId);
		System.out.println("The db to delete id is " + dbId + " it has" + db.getTables().size());

		Set<CatalogTable> tables = db.getTables();
		for(CatalogTable temp : tables) {
			System.out.println("table name is " + temp.getTableName());
		}
		table = db.getTable("table-1");
		db.removeTable(table);
		repo.updateDatabase(db);


		db = repo.getDatabase(dbId);  // this updates the hibernate object
		repo.dropDatabase(db);
	}

	@Test()
	public void
	generateSQLTest() throws Exception {
		System.out.println(
			"=============================== GENERATE RELATIONAL ALGEBRA TEST ====================================");
		final long startTime = System.currentTimeMillis();


		CatalogDatabaseImpl db = new CatalogDatabaseImpl("main");

		repo.createDatabase(db);
		dbId = db.getId();

		CatalogColumnImpl column1 = new CatalogColumnImpl("col1", CatalogColumnDataType.INT64, 1);
		CatalogColumnImpl column2 = new CatalogColumnImpl("col2", CatalogColumnDataType.INT32, 2);
		CatalogColumnImpl column3 = new CatalogColumnImpl("col3", CatalogColumnDataType.INT32, 2);

		List<CatalogColumnImpl> columns = new ArrayList<CatalogColumnImpl>();
		columns.add(column1);
		columns.add(column2);
		columns.add(column3);

		CatalogTableImpl table = new CatalogTableImpl("table1", db, columns);

		db.addTable(table);
		repo.updateDatabase(db);

		final long endTime = System.currentTimeMillis();


		db = repo.getDatabase(dbId);
		System.out.println("Total execution time: " + (endTime - startTime));
		System.out.println("The db to delete id is " + dbId + " it has" + db.getTables().size());

		BlazingSchema schema = new BlazingSchema(db);
		Table tableTemp = schema.getTable("table1");
		if(tableTemp == null) {
			System.out.println("table NOT found");
			throw new Exception();
		} else {
			System.out.println("table found");
		}
		RelationalAlgebraGenerator algebraGen = new RelationalAlgebraGenerator(schema);

		RelNode node = algebraGen.getRelationalAlgebra("select col1 + 2 from `table1` where col2 > 4");

		repo.dropDatabase(db);
		// TODO: some kind of assertion that we got the reight relational algebra
	}

	@Test()
	public void
	testLoadDataInFile()
		throws RelConversionException, ValidationException, SqlParseException, IOException, SQLException {
		/*		System.out.println("Hello, World");

				System.out.println("testSaveOperation begins ........ This is \"C\" of CRUD");

				CatalogColumnDataType type1 = CatalogColumnDataType.FLOAT64;

				Session session = sessionFactory.openSession();
				session.beginTransaction();

				session.save(type1);

				session.getTransaction().commit();
				session.close();
				System.out.println("testSaveOperation ends .......");

				System.out.println("BYE, World");*/
	}
}
