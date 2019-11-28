package com.blazingdb.calcite.catalog.repository;

import org.hibernate.Criteria;
import org.hibernate.Hibernate;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Restrictions;

import com.blazingdb.calcite.catalog.domain.CatalogDatabaseImpl;
import com.blazingdb.calcite.catalog.domain.CatalogSchema;
import com.blazingdb.calcite.catalog.domain.CatalogSchemaImpl;
import com.blazingdb.calcite.catalog.domain.CatalogTableImpl;

public class DatabaseRepository {
	/**
	 * The hibernate session we are opening
	 */
	private Session sessionObj = null;
	
	/**
	 * Constructor initializes the hibernate session
	 */
	public DatabaseRepository() {
		sessionObj = getSessionFactory().openSession();
	}
	
	/**
	 * Closes the hibernate session
	 */
	  @Override
	  public void finalize() {
	   sessionObj.close();
	  }
	
	  /**
	   * Creates a SessionFactory
	   * @return A session factory capable of opening sessions
	   */
	private static SessionFactory getSessionFactory() {
		Configuration configuration = new Configuration().configure();
		configuration = configuration.setProperty("hibernate.connection.url", "jdbc:h2:mem:blazingsql");
		SessionFactory sessionFactory = configuration.buildSessionFactory();
		return sessionFactory;
	}
	
	/**
	 * Persists a database using hibernate. Commits and flushes.
	 * @param database The database to persist
	 */
	public void createDatabase(CatalogDatabaseImpl database) {
		Transaction transObj = null;
		try {
			transObj = sessionObj.beginTransaction();
			sessionObj.persist(database);
			transObj.commit();
			sessionObj.flush();
		} catch (HibernateException exObj) {
			if(transObj!=null){
				transObj.rollback();
			}
			exObj.printStackTrace(); 
		} 
	}
	/**
	 * Gets a database according to its id column.
	 * @param dbId id of the database to be returned.
	 * @return A database if it exists else null
	 */
	public CatalogDatabaseImpl getDatabase(Long dbId) {
		Transaction transObj = null;
		try {
			CatalogDatabaseImpl db = (CatalogDatabaseImpl) sessionObj.load(CatalogDatabaseImpl.class, dbId);
			Hibernate.initialize(db);
			return db;
		} catch (HibernateException exObj) {
			exObj.printStackTrace(); 
		} 
		return null;
	}
	/**
	 * Deletes a database using hibernate. Commits and flushes.
	 * @param database the database to be deleted
	 */
	public void dropDatabase(CatalogDatabaseImpl database) {
		Transaction transObj = null;
		try {
			transObj = sessionObj.beginTransaction();
			sessionObj.delete(database);
			transObj.commit();
			sessionObj.flush();
		} catch (HibernateException exObj) {
			if(transObj!=null){
				transObj.rollback();
			}
			exObj.printStackTrace(); 
		} 
	}
	/*
	public void createTable(CatalogTableImpl table) {
		Transaction transObj = null;
		try {
			transObj = sessionObj.beginTransaction();
			sessionObj.persist(table);
			transObj.commit();
			sessionObj.flush();

		} catch (HibernateException exObj) {
			if(transObj!=null){
				transObj.rollback();
			}
			exObj.printStackTrace(); 
		} 
	}
	public void dropTable(CatalogTableImpl table) {
		Transaction transObj = null;
		try {
			transObj = sessionObj.beginTransaction();
			sessionObj.delete(table);
			transObj.commit();
			sessionObj.flush();
		} catch (HibernateException exObj) {
			if(transObj!=null){
				transObj.rollback();
			}
			exObj.printStackTrace(); 
		}
	}
*/
	/**
	 * Gets a database by name.
	 * @param dbName The name of the database to be retrieved
	 * @return a database whose name matches the one provided
	 */
	public CatalogDatabaseImpl getDatabase(String dbName) {
		
		Transaction transObj = null;
		try {
			
			Criteria criteria = sessionObj.createCriteria(CatalogDatabaseImpl.class);
			CatalogDatabaseImpl db  = (CatalogDatabaseImpl) criteria.add(Restrictions.eq("name", dbName))
			                             .uniqueResult();
	
			Hibernate.initialize(db);
			return db;
		} catch (HibernateException exObj) {
			exObj.printStackTrace(); 
		} 
		return null;
	}

	//ironically right now these are teh same
	public void updateDatabase(CatalogDatabaseImpl db) {
		createDatabase(db);
	}
	
}
