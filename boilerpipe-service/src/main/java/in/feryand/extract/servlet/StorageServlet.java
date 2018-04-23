package in.feryand.extract.servlet;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.stream.Collectors;

@SuppressWarnings("serial")
public class StorageServlet extends HttpServlet {
  private Storage storage = StorageOptions.getDefaultInstance().getService();
  private final static String BUCKET = "actadiurna";
  private final static String BASEDIR = "articles/json/";

  @Override
  @SuppressWarnings("unchecked")
  public final void doGet(final HttpServletRequest req,
                           final HttpServletResponse resp)
      throws IOException {
    JsonObject response = new JsonObject();
    response.add("status", new JsonPrimitive("success"));
    response.add("data", new JsonObject());
    response.getAsJsonObject("data").add("objects", new JsonArray());

    String directory = req.getParameter("directory");
    Page<Blob> blobs = storage.list(BUCKET, Storage.BlobListOption.currentDirectory(),
        Storage.BlobListOption.prefix(BASEDIR + directory));

    Iterable<Blob> blobIterator = blobs.iterateAll();
    for (Blob blob : blobIterator) {
      JsonObject oblob = new JsonObject();
      oblob.add("name", new JsonPrimitive(blob.getName()));
      oblob.add("isDirectory", new JsonPrimitive(blob.isDirectory()));
      response
          .getAsJsonObject("data")
          .getAsJsonArray("objects")
          .add(oblob);
    }

    resp.setHeader("Content-Type", "application/json");
    resp.setStatus(200);
    PrintWriter out = resp.getWriter();
    out.println(response.toString());
  }

  @Override
  @SuppressWarnings("unchecked")
  public final void doPost(final HttpServletRequest req,
                          final HttpServletResponse resp)
      throws IOException {
    if (!req.getHeader("Content-Type").equals("application/json")) {
      throw new IOException();
    }

    String postData = req.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
    JsonObject data = new JsonParser().parse(postData).getAsJsonObject();

    if (!data.has("objects")) {
      throw new IOException();
    }

    JsonArray objects = data.get("objects").getAsJsonArray();

  }

}
