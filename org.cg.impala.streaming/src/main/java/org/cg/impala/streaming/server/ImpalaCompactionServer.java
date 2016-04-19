package org.cg.impala.streaming.server;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;
import org.cg.impala.streaming.CompactionManager;
import org.cg.impala.streaming.compaction.CompactionContext;
import org.cg.impala.streaming.compaction.Table;
import org.cg.impala.streaming.compaction.View;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;

import akka.http.javadsl.marshallers.jackson.Jackson;

import akka.http.javadsl.server.Handler;
import akka.http.javadsl.server.Handler1;
import akka.http.javadsl.server.HttpApp;
import akka.http.javadsl.server.RequestContext;
import akka.http.javadsl.server.RequestVal;
import akka.http.javadsl.server.RequestVals;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.server.RouteResult;
import akka.http.javadsl.server.Unmarshallers;
import akka.http.javadsl.server.values.Parameter;
import akka.http.javadsl.server.values.Parameters;



public class ImpalaCompactionServer extends HttpApp {

	private CompactionManager compactionManager;

	private static final Log logger = LogFactory.getLog(ImpalaCompactionServer.class);

	private static Parameter<String> table = Parameters.stringValue("table");

	private static String COMPACTION_CONFIG_NAME = "compactionConfigFile";

	static RequestVal<String> bodyAsName = RequestVals.entityAs(Unmarshallers.String());

	Config config;

	public ImpalaCompactionServer(Config config) {
		this.config = config;
		initialize();
	
		
	}

	public void initialize() {

		String compactionConfigFile = config.getString(COMPACTION_CONFIG_NAME);

		try {
			compactionManager = new CompactionManager(compactionConfigFile);
		} catch (ClassNotFoundException e) {
			logger.fatal("error initializing server", e);

		} catch (IOException e) {
			logger.fatal("error initializing server", e);

		} catch (SQLException e) {
			logger.fatal("error initializing server", e);

		}
	}

	@Override
	public Route createRoute() {


		Handler1<String> addTableHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;

			
			public RouteResult apply(RequestContext ctx, String tableName) {
				try {
					compactionManager.addTable(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IOException e) {
					return ctx.complete(Responses.InternalErrorResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				}
				return ctx.completeAs(Jackson.json(), tableName + " added ");
			}
		};

		Handler listTableHandler = new Handler() {
			private static final long serialVersionUID = 1L;

		
			public RouteResult apply(RequestContext ctx) {
				List<String> tables = new ArrayList<String>();
				try {
					tables = compactionManager.listTables();
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IOException e) {
					return ctx.complete(Responses.InternalErrorResponse(e));
				}
				return ctx.completeAs(Jackson.json(), tables);
			}
		};

		Handler1<String> getTableStateHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;

			public RouteResult apply(RequestContext ctx, String tableName) {
				CompactionContext state = null;
				try {
					state = compactionManager.getTableState(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				}
				return ctx.completeAs(Jackson.json(), state);
			}
		};

		Handler1<String> getTableViewHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;

	
			public RouteResult apply(RequestContext ctx, String tableName) {
				View view = null;
				try {
					view = compactionManager.getView(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				}
				return ctx.completeAs(Jackson.json(), view);
			}
		};

		Handler1<String> getTableLandingTableHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;

	
			public RouteResult apply(RequestContext ctx, String tableName) {
				Table landingTable = null;
				try {
					landingTable = compactionManager.getLandingTable(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				}
				return ctx.completeAs(Jackson.json(), landingTable);
			}
		};

		Handler1<String> goNextHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;


			public RouteResult apply(RequestContext ctx, String tableName) {
				try {
					compactionManager.runNext(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IOException e) {
					return ctx.complete(Responses.InternalErrorResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				} catch (InterruptedException e) {
					return ctx.complete(Responses.InternalErrorResponse(e));
				}
				return ctx.completeAs(Jackson.json(), tableName + " next step fnished ");
			}
		};

		Handler1<String> compactionHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;

			public RouteResult apply(RequestContext ctx, String tableName) {
				try {
					compactionManager.compaction(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IOException e) {
					return ctx.complete(Responses.InternalErrorResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				} catch (InterruptedException e) {
					return ctx.complete(Responses.InternalErrorResponse(e));
				}
				return ctx.completeAs(Jackson.json(), tableName + " compaction finished ");
			}
		};

		Handler1<String> loadHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;
			
			public RouteResult apply(RequestContext ctx, String tableName) {
				try {
					compactionManager.load(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				}
				return ctx.completeAs(Jackson.json(), tableName + " loaded ");
			}
		};

		return route(
				// matches the empty path
				pathSingleSlash().route(get(complete("Impala Compaction Service"))),
				
				path("add").route(post(handleWith1(table, addTableHandler))),
				path("list").route(get(handleWith(listTableHandler))),
				path("state").route(get(handleWith1(table, getTableStateHandler))),
				path("view").route(get(handleWith1(table, getTableViewHandler))),
				path("landing").route(get(handleWith1(table, getTableLandingTableHandler))),
				path("next").route(post(handleWith1(table, goNextHandler))),
				path("compaction").route(post(handleWith1(table, compactionHandler))),
				path("load").route(post(handleWith1(table, loadHandler))));
	}

	public static void main(String[] args) throws IOException {
		BasicConfigurator.configure();
		File file = new File("conf/properties.conf");
		Config config = ConfigFactory.parseFile(file);
		config.resolve();
		String host = config.getString("host");
		int port = config.getInt("port");
		ActorSystem system = ActorSystem.create("ImpalaCompactionServer", config);
		new ImpalaCompactionServer(config).bindRoute(host, port, system);

	}

}