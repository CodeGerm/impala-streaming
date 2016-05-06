package org.cg.impala.streaming.server;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cg.impala.streaming.CompactionManager;
import org.cg.impala.streaming.compaction.CompactionContext;
import org.cg.impala.streaming.compaction.CompactionStatus;
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
	
	private static Parameter<String> id = Parameters.stringValue("id");

	private static String COMPACTION_CONFIG_NAME = "compactionConfigFile";

	static RequestVal<String> bodyAsName = RequestVals.entityAs(Unmarshallers.String());

	Config config;

	public ImpalaCompactionServer(Config config) {
		this.config = config;
		initialize();


	}

	public void initialize() {

		String compactionConfigFile = config.getString(COMPACTION_CONFIG_NAME);
		logger.info("starting server");
		
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
				logger.info( tableName + " added ");
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
				logger.info( "tables: "+tables );
				return ctx.completeAs(Jackson.json(), tables);
			}
		};

		Handler1<String> getTableStateHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;

			public RouteResult apply(RequestContext ctx, String tableName) {
				String state = null;
				try {
					state = compactionManager.getTableState(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				}
				logger.info( "state: "+state );
				return ctx.completeAs(Jackson.json(), state);
			}
		};

		Handler1<String> getTableContextHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;

			public RouteResult apply(RequestContext ctx, String tableName) {
				CompactionContext context = null;
				try {
					context = compactionManager.getTableContext(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				}
				logger.info( "Context: "+context );
				return ctx.completeAs(Jackson.json(), context);
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
				logger.info( "view: "+view );
				return ctx.completeAs(Jackson.json(), view);
			}
		};

		Handler1<String> getTableLandingTableHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;


			public RouteResult apply(RequestContext ctx, String tableName) {
				String landingTable = null;
				try {
					landingTable = compactionManager.getLandingTable(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				}
				logger.info( "landingTable: "+landingTable );
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
				logger.info( tableName + " next step fnished " );
				return ctx.completeAs(Jackson.json(), tableName + " next step fnished ");
			}
		};

		Handler1<String> compactionHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;

			public RouteResult apply(RequestContext ctx, String tableName) {
				CompactionStatus newLandingTable = null;
				try {
					newLandingTable = compactionManager.compaction(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IOException e) {
					return ctx.complete(Responses.InternalErrorResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				} catch (InterruptedException e) {
					return ctx.complete(Responses.InternalErrorResponse(e));
				} catch (IllegalStateException e) {
					return ctx.complete(Responses.InternalErrorResponse(e));
				}

				logger.info( tableName + " compaction started " );
				return ctx.completeAs(Jackson.json(), newLandingTable);

			}
		};
		
		Handler1<String> recoverHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;

			public RouteResult apply(RequestContext ctx, String tableName) {
				
				try {
					compactionManager.recover(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IOException e) {
					return ctx.complete(Responses.InternalErrorResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				} catch (InterruptedException e) {
					return ctx.complete(Responses.InternalErrorResponse(e));
				} catch (IllegalStateException e) {
					return ctx.complete(Responses.InternalErrorResponse(e));
				}

				logger.info( tableName + " recovery finished " );
				return ctx.completeAs(Jackson.json(), " recovery finished ");

			}
		};
		
		Handler1<String> compactionStatusHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;

			public RouteResult apply(RequestContext ctx, String id) {
				CompactionStatus cs = null;
				try {
					cs = compactionManager.getCompactionStatus(id);
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				}
				logger.info( " compaction status: " + cs );
				return ctx.completeAs(Jackson.json(), cs);

			}
		};

		Handler1<String> loadHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;

			public RouteResult apply(RequestContext ctx, String tableName) {
				try {
					//prevent potential problem caused by concurrent altering table
					//if(!compactionManager.getTableState(tableName).equals(CompactionContext.States.StateI.toString()))
					//	return ctx.complete(Responses.InternalErrorResponse("Can't load during compaction, try again later"));
					compactionManager.load(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				} catch (IOException e) {
					return ctx.complete(Responses.InternalErrorResponse(e));
				} 
				logger.info(  tableName + " loaded " );
				return ctx.completeAs(Jackson.json(), tableName + " loaded ");
			}
		};
		
		
		Handler1<String> isLoadingHandler = new Handler1<String>() {
			private static final long serialVersionUID = 1L;

			public RouteResult apply(RequestContext ctx, String tableName) {
				Boolean isLoading = null;
				try {
					isLoading = compactionManager.getLoadingState(tableName);
				} catch (SQLException e) {
					return ctx.complete(Responses.BadRequestResponse(e));
				} catch (IllegalArgumentException e) {
					return ctx.complete(Responses.NotFoundResponse(e));
				} 
				logger.info(  " is loading: "+ isLoading );
				return ctx.completeAs(Jackson.json(), isLoading);
			}
		};

		return route(
				// matches the empty path
				pathSingleSlash().route(get(complete("Impala Compaction Service"))),

				path("add").route(post(handleWith1(table, addTableHandler))),
				path("list").route(get(handleWith(listTableHandler))),
				path("state").route(get(handleWith1(table, getTableStateHandler))),
				path("status").route(get(handleWith1(id, compactionStatusHandler))),
				path("isloading").route(get(handleWith1(table, isLoadingHandler))),
				path("context").route(get(handleWith1(table, getTableContextHandler))),
				path("view").route(get(handleWith1(table, getTableViewHandler))),
				path("landing").route(get(handleWith1(table, getTableLandingTableHandler))),
				path("next").route(post(handleWith1(table, goNextHandler))),
				path("compaction").route(post(handleWith1(table, compactionHandler))),
				path("recover").route(post(handleWith1(table, recoverHandler))),
				path("load").route(post(handleWith1(table, loadHandler))));
	}

	public static void main(String[] args) throws IOException {

		if(args.length==0){
			System.out.println("Please provide config file path");
			System.exit(0);
		}
		String  configPath = args[0];
		File file = new File(configPath);
		Config config = ConfigFactory.parseFile(file);
		config.resolve();
		String host = config.getString("host");
		int port = config.getInt("port");
		ActorSystem system = ActorSystem.create("ImpalaCompactionServer", config);
		new ImpalaCompactionServer(config).bindRoute(host, port, system);
	}

}