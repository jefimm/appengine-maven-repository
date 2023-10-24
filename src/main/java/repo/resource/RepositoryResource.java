package repo.resource;


import com.google.api.gax.paging.Page;
import com.google.appengine.api.utils.SystemProperty;
import com.google.cloud.storage.*;
import org.glassfish.jersey.server.mvc.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import repo.Application;
import repo.annotation.CacheControl;
import repo.model.Directory;
import repo.model.FileContext;

import javax.annotation.security.RolesAllowed;
import javax.inject.Singleton;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Date;

import static repo.Application.*;

@Path("/")
@Singleton
public class RepositoryResource {
    static private final Logger LOGGER = LoggerFactory.getLogger(RepositoryResource.class);

    private static final String DEFAULT_BUCKET = SystemProperty.applicationId.get() + ".appspot.com";
    private static final String BUCKET_NAME = System.getProperty(repo.Application.PROPERTY_BUCKET_NAME, DEFAULT_BUCKET);
    private static final Boolean UNIQUE_ARTIFACTS = Boolean.parseBoolean(System.getProperty(Application.PROPERTY_UNIQUE_ARTIFACT, "false"));

    private final Storage storage = StorageOptions.getDefaultInstance().getService();

    @GET
    @Path("/_ah/start")
    public Response startup() {
        return Response.accepted().build();
    }

    @GET
    @Template(name = "/list.mustache")
    @RolesAllowed(value = {ROLE_WRITE, ROLE_READ, ROLE_LIST})
    @CacheControl(property = Application.PROPERTY_CACHE_CONTROL_LIST)
    @Produces(MediaType.TEXT_HTML)
    public Directory list(@Context UriInfo uriInfo) throws IOException {
        return list("", uriInfo);
    }

    @GET
    @Path("{dir: .*[/]}")
    @Template(name = "/list.mustache")
    @RolesAllowed(value = {ROLE_WRITE, ROLE_READ, ROLE_LIST})
    @CacheControl(property = Application.PROPERTY_CACHE_CONTROL_LIST)
    @Produces(MediaType.TEXT_HTML)
    public Directory list(@PathParam("dir") final String dir,
                          @Context final UriInfo uriInfo) throws IOException {
        final Storage.BlobListOption opt = Storage.BlobListOption.prefix(dir);
        final Page<Blob> list = storage.list(BUCKET_NAME, opt, Storage.BlobListOption.currentDirectory());

        final Directory.Builder directory = Directory.builder(URI.create(uriInfo.getPath()));

        for (Blob file : list.getValues()) {
            final String name = file.getName();

            if (name.equals(dir)) {
                continue;
            }

            directory.add(new FileContext(name.substring(dir.length()), file.getSize(),
                    getCreateDate(file),
                    file.isDirectory()));
        }
        if (!dir.isEmpty() && directory.isEmpty()) {
            throw new NotFoundException();
        }

        return directory.build();
    }

    @GET
    @Path("{file: .*}")
    @RolesAllowed(value = {ROLE_WRITE, ROLE_READ})
    @CacheControl(property = Application.PROPERTY_CACHE_CONTROL_FETCH)
    public Response fetch(@PathParam("file") String filename, @Context Request request) throws IOException {

        final Blob fileData = storage.get(BUCKET_NAME, filename);

        if (fileData == null) {
            throw new NotFoundException();
        }

        final EntityTag etag = new EntityTag(fileData.getEtag());
        final Date lastModified = getCreateDate(fileData);

        final String mimeType = fileData.getContentType();

        Response.ResponseBuilder response = request.evaluatePreconditions(lastModified, etag);

        if (response == null) {
            StreamingOutput fileStream = output -> {
                try {
                    fileData.downloadTo(output);
                    output.flush();
                } catch (Exception e) {
                    throw new WebApplicationException(e.getMessage());
                }
            };
            final String fname = java.nio.file.Path.of(fileData.getName()).getFileName().toString();
            response = Response
                    .ok(fileStream, mimeType == null ? MediaType.APPLICATION_OCTET_STREAM_TYPE.toString() : mimeType)
                    .header("content-disposition", "attachment; filename = " + fname)
                    .tag(etag)
                    .lastModified(lastModified);
        } else {
            if (mimeType != null) {
                response.type(mimeType);
            }
        }

        return response.build();
    }

    @PUT
    @Path("{file: .*}")
    @RolesAllowed(ROLE_WRITE)
    public Response put(@PathParam("file") String filename,
                        @HeaderParam(HttpHeaders.CONTENT_TYPE) String mimeType,
                        byte[] content) throws IOException {

        if (UNIQUE_ARTIFACTS && gcsFileExist(filename) && isNotAMavenFile(filename)) {
            String duplicate_artifact_warning = "The uploaded artifact is already inside the repository. If you want to overwrite the artifact, you have to disable the 'repository.unique.artifact' flag";
            LOGGER.info(duplicate_artifact_warning);
            return Response.notAcceptable(null).entity(duplicate_artifact_warning).build();
        }
        BlobId blobId = BlobId.of(BUCKET_NAME, filename);
        BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId);
        if (mimeType != null) {
            blobInfoBuilder.setContentType(mimeType);
        }

        storage.create(blobInfoBuilder.build(), content);
        return Response.accepted().build();
    }

    private boolean isNotAMavenFile(String file) {
        return !file.endsWith("maven-metadata.xml") && !file.endsWith("maven-metadata.xml.sha1") && !file.endsWith("maven-metadata.xml.md5");
    }

    private boolean gcsFileExist(String filename) throws IOException {
        return storage.get(BUCKET_NAME, filename) != null;
    }

    public static Date getCreateDate(Blob fileData) {
        OffsetDateTime dt = fileData.getCreateTimeOffsetDateTime();
        return dt == null ? null : new Date(dt.toInstant().toEpochMilli());
    }
}
