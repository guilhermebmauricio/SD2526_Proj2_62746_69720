package sd2526.trab.impl.api.rest;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import sd2526.trab.api.Message;
import sd2526.trab.impl.replication.ReplicationAck;
import sd2526.trab.impl.replication.ReplicationCatchupResponse;
import sd2526.trab.impl.replication.ReplicationOperation;

public interface RestReplicationMessages {
	final String REPLICATION = "/replication";
	final String VERSION = "version";
	final String AFTER = "after";
	final String LIMIT = "limit";

	@GET
	@Path(REPLICATION + "/" + VERSION)
	@Produces(MediaType.APPLICATION_JSON)
	Long getCurrentVersion();

	@POST
	@Path(REPLICATION)
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	ReplicationAck replicateOperation(ReplicationOperation op);

	@GET
	@Path(REPLICATION)
	@Produces(MediaType.APPLICATION_JSON)
	ReplicationCatchupResponse getOperationsAfter(@QueryParam(AFTER) long seq, @QueryParam(LIMIT) @DefaultValue("100") int limit);

}
