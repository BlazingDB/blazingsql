/*
 * This file is part of the JNR project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.blazingdb.calcite.application;

/**
 * Class which holds main function. Listens in on a TCP socket
 * for protocol buffer requests and then processes these requests.
 * @author felipe
 *
 */

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.dbcp2.BasicDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

import java.nio.ByteBuffer;

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

public class CalciteApplication {
	final static Logger LOGGER = LoggerFactory.getLogger(CalciteApplication.class);

	public static void
	executeUpdate() throws NamingException, SQLException, LiquibaseException, InstantiationException,
						   IllegalAccessException, ClassNotFoundException {
		// setDataSource((String) servletValueContainer.getValue(LIQUIBASE_DATASOURCE));

		final String LIQUIBASE_CHANGELOG = "liquibase.changelog";
		final String LIQUIBASE_DATASOURCE = "liquibase.datasource";

		String changeLogFile;
		String contexts;
		String labels;

		// setChangeLogFile((String) servletValueContainer.getValue(LIQUIBASE_CHANGELOG));
		changeLogFile = "liquibase-bz-master.xml";

		// setContexts((String) servletValueContainer.getValue(LIQUIBASE_CONTEXTS));
		contexts = "";
		// setLabels((String) servletValueContainer.getValue(LIQUIBASE_LABELS));

		labels = "";

		// defaultSchema = StringUtil.trimToNull((String)
		// servletValueContainer.getValue(LIQUIBASE_SCHEMA_DEFAULT));
		// defaultSchema =

		Connection connection = null;
		Database database = null;
		BasicDataSource dataSource = null;
		try {
			// DriverManager.registerDriver((Driver) Class.forName("com.mysql.jdbc.Driver").newInstance());
			// String url = "jdbc:mysql://localhost:3306/bz3";
			// connection = DriverManager.getConnection(url);

			dataSource = new BasicDataSource();
			dataSource.setDriverClassName("org.h2.Driver");
			dataSource.setUsername("blazing");
			dataSource.setPassword("blazing");
			dataSource.setUrl("jdbc:h2:mem:blazingsql");
			dataSource.setMaxTotal(10);
			dataSource.setMaxIdle(5);
			dataSource.setInitialSize(5);
			dataSource.setValidationQuery("SELECT 1");

			// MySQLData dataSource = new JdbcDataSource(); // (DataSource) ic.lookup(dataSourceName);
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

			Liquibase liquibase =
				new Liquibase(changeLogFile, new CompositeResourceAccessor(clFO, fsFO, threadClFO), database);

			// @SuppressWarnings("unchecked")
			// StringTokenizer initParameters = new StringTokenizer(""); // servletContext.getInitParameterNames();
			// while (initParameters.hasMoreElements()) {
			// String name = initParameters.nextElement().trim();
			// if (name.startsWith(LIQUIBASE_PARAMETER + ".")) {
			// // liquibase.setChangeLogParameter(name.substring(LIQUIBASE_PARAMETER.length() + 1),
			// // servletValueContainer.getValue(name));
			// }
			// }

			liquibase.update(new Contexts(contexts), new LabelExpression(labels));
		} finally {
			if(database != null) {
				database.close();
			} else if(connection != null) {
				connection.close();
			}
		}
	}

	public static int
	bytesToInt(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		buffer.order(LITTLE_ENDIAN);
		return buffer.getInt();
	}

	public static byte[] intToBytes(int value) { return ByteBuffer.allocate(4).putInt(value).array(); }

	private static CalciteApplicationOptions
	parseArguments(String[] arguments) {
		final Options options = new Options();

		final String tcpPortDefaultValue = "8891";
		final Option tcpPortOption = Option.builder("p")
										 .required(false)
										 .longOpt("port")
										 .hasArg()
										 .argName("INTEGER")
										 .desc("TCP port for this service")
										 .type(Integer.class)
										 .build();
		options.addOption(tcpPortOption);

		final String orchestratorIpDefaultValue = "127.0.0.1";
		final Option orchestratorIp = Option.builder("oip")
										  .required(false)
										  .longOpt("orch_ip")
										  .hasArg()
										  .argName("HOSTNAME")
										  .desc("Orchestrator hostname")
										  .build();
		options.addOption(orchestratorIp);

		final String orchestratorPortDefaultValue = "8889";
		final Option orchestratorPort = Option.builder("oport")
											.required(false)
											.longOpt("orch_port")
											.hasArg()
											.argName("INTEGER")
											.desc("Orchestrator port")
											.build();
		options.addOption(orchestratorPort);

		try {
			final CommandLineParser commandLineParser = new DefaultParser();
			final CommandLine commandLine = commandLineParser.parse(options, arguments);

			final Integer tcpPort =
				Integer.valueOf(commandLine.getOptionValue(tcpPortOption.getLongOpt(), tcpPortDefaultValue));
			CalciteApplicationOptions calciteApplicationOptions = null;
			final String orchIp = commandLine.getOptionValue(orchestratorIp.getLongOpt(), orchestratorIpDefaultValue);
			final Integer orchPort = Integer.valueOf(
				commandLine.getOptionValue(orchestratorPort.getLongOpt(), orchestratorPortDefaultValue));

			calciteApplicationOptions = new CalciteApplicationOptions(tcpPort, orchIp, orchPort);

			return calciteApplicationOptions;
		} catch(ParseException e) {
			LOGGER.error(e.getMessage());
			final HelpFormatter helpFormatter = new HelpFormatter();
			helpFormatter.printHelp("CalciteApplication", options);
			System.exit(1);
			return null;
		}
	}

	private static class CalciteApplicationOptions {
		private Integer tcpPort = null;
		private String orchestratorIp = null;
		private Integer orchestratorPort = null;

		public CalciteApplicationOptions(
			final Integer tcpPort, final String orchestratorIp, final Integer orchestratorPort) {
			this.tcpPort = tcpPort;
			this.orchestratorIp = orchestratorIp;
			this.orchestratorPort = orchestratorPort;
		}

		public Integer
		tcpPort() {
			return this.tcpPort;
		}

		public String
		getOrchestratorIp() {
			return orchestratorIp;
		}

		public Integer
		getOrchestratorPort() {
			return orchestratorPort;
		}
	}
}
