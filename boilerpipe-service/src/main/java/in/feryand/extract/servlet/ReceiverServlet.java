package in.feryand.extract.servlet;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.appengine.datastore.AppEngineDataStoreFactory;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.appengine.api.datastore.*;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.cloud.storage.*;
import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;

import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import in.feryand.extract.helpers.DriveHelper;
import in.feryand.extract.helpers.TimeHelper;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.*;
import java.util.zip.*;

@SuppressWarnings("serial")
public class ReceiverServlet extends HttpServlet {
  private Storage storage = StorageOptions.getDefaultInstance().getService();

  /* Google Drive */
  private static final String APPLICATION_NAME = "Hoax Analyzer Project: Artikel";
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final String CLIENT_SECRET_DIR = "WEB-INF/client_secret.json";

  /* Google Drive Credentials */
  private static final List<String> SCOPES = Collections.singletonList(DriveScopes.DRIVE);

  private static final String rootFolderId = "12KdmXUxssdL5nKYz5afolHqHEW_fdCFI";
  private static final String jsonRootFolderId = "1y0MNe60KyP71aeP_eAbFJhmjeT4CUVnB";
  private static final String htmlRootFolderId = "1CwgMxDMnm53pUClG8XKvLOqJ1ZMCv-j8";

  /* Google Cloud Storage */
  private static final String BUCKET = "actadiurna";
  private static final String BACKUP_BUCKET = "actadiurna-data";
  private static final String BASE = "articles/";

  private static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT, JsonFactory JSON_FACTORY) throws IOException, GeneralSecurityException {
    InputStream in = new FileInputStream(new File(CLIENT_SECRET_DIR));
    GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

    GoogleCredential credential = new GoogleCredential.Builder()
        .setTransport(HTTP_TRANSPORT)
        .setJsonFactory(JSON_FACTORY)
        .setClientSecrets(clientSecrets).build();
    credential.setRefreshToken("1/_g9DkpI_LPHtF144p2T5lWQF797ndcmDGQ_NwXIGl9c")
        .setAccessToken("ya29.GluzBczXa1KWcIrp29M_1FH5VxjvS5sp8qGchMrbus1XLf2ATvUm-8qi5SqllBfcMSZxFsSqcfM3WCU260FlJudzwlgEI5MvkFCgMaZNBnnGbURX6JGMNG6r_aY5");

    return credential;
  }

  @Override
  @SuppressWarnings("unchecked")
  public final void doPost(final HttpServletRequest req,
                           final HttpServletResponse resp)
      throws IOException {
    TimeHelper th = TimeHelper.getInstance();
    DriveHelper dh = DriveHelper.getInstance();

    MemcacheService memcacheService =
        MemcacheServiceFactory.getMemcacheService();

    DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();

    String subscriptionToken = "th15isS3cr3Tt0lx3n";
    if (!subscriptionToken.equals(req.getParameter("token"))) {
      resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      resp.getWriter().close();
      return;
    }

    ServletInputStream inputStream = req.getInputStream();

    JsonParser parser = JacksonFactory.getDefaultInstance()
        .createJsonParser(inputStream);
    parser.skipToKey("message");
    PubsubMessage message = parser.parseAndClose(PubsubMessage.class);
    Map<String, String> attributes = message.getAttributes();

    String payload = new String(message.decodeData());
    com.google.gson.JsonParser jsonParser = new com.google.gson.JsonParser();
    JsonObject object = (JsonObject) jsonParser.parse(payload);

    byte[] decoded = Base64.getDecoder().decode(object.get("html").getAsString());

    try {
      String html = new String(decompress(decoded));

      // String article = ArticleExtractor.INSTANCE.getText(html);
      String article = object.get("content").getAsString();
      if (article.length() < 10) {
        throw new IOException();
      }

      String filename = getFilePath(attributes.get("title"));
      String datePath = th.getDatePath(attributes.get("date"));
      String timestamp = th.getTimestamp(attributes.get("date"));
      String gcsFullPath = datePath + "/" + timestamp + "." + filename;

      /* BACKUP PUBSUB DATA */
      createGcsFile(
          BACKUP_BUCKET,
          "json/" + gcsFullPath + ".message.json",
          pubsubToJsonString(message).getBytes()
      );

      /* HTML */
      createGcsFile(
          BUCKET,
          BASE + "html/" + gcsFullPath + ".html",
          html.getBytes()
      );

      /* JSON */
      createGcsFile(
          BUCKET,
          BASE + "json/" + gcsFullPath + ".json",
          articleToJsonString(
              attributes.get("url"),
              attributes.get("date"),
              attributes.get("referrer"),
              attributes.get("title"),
              article,
              gcsFullPath,
              true
          ).getBytes()
      );

      /* GOOGLE DRIVE */

      final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT, JSON_FACTORY))
          .setApplicationName(APPLICATION_NAME)
          .build();

      String htmlFolderId = getFolderId(
          datastore,
          service,
          htmlRootFolderId,
          "html",
          datePath,
          attributes.get("date"));

      String jsonFolderId = getFolderId(
          datastore,
          service,
          jsonRootFolderId,
          "json",
          datePath,
          attributes.get("date"));

      /* GOOGLE DRIVE - HTML */
      String fileResult = "";
      fileResult = dh.uploadFileToDrive(
          service,
          htmlFolderId,
          timestamp + "." + filename,
          ".html",
          html
      );
      System.out.println("HTML Upload: " + fileResult);

      /* GOOGLE DRIVE - JSON */
      fileResult = dh.uploadFileToDrive(
          service,
          jsonFolderId,
          timestamp + "." + filename,
          ".json",
          articleToJsonString(
              attributes.get("url"),
              attributes.get("date"),
              attributes.get("referrer"),
              attributes.get("title"),
              article,
              "",
              false
          )
      );
      System.out.println("JSON Upload: " + fileResult);

    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException();
    }

    memcacheService.delete("messageCache");

    resp.setStatus(HttpServletResponse.SC_OK);
    resp.getWriter().close();
  }

  private void createGcsFile(String bucket, String path, byte[] data) {
    BlobId blobId = BlobId.of(bucket, path);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    storage.create(blobInfo, data);
  }

  private String articleToJsonString(
      String url,
      String date,
      String referrer,
      String title,
      String content,
      String filepath,
      Boolean html) {
    Gson gson = new Gson();
    JsonObject data = new JsonObject();
    data.addProperty("url", url);
    data.addProperty("date", date);
    data.addProperty("referrer", referrer);
    data.addProperty("title", title);
    data.addProperty("content", content);
    if (html) {
      data.addProperty("html", "https://storage.googleapis.com/actadiurna/" + BASE + "html/" + filepath + ".html");
    }
    return gson.toJson(data);
  }

  private String pubsubToJsonString(PubsubMessage message) {
    Gson gson = new Gson();
    JsonObject data = new JsonObject();
    data.addProperty("message", new String(message.decodeData()));
    data.add("attributes", gson.toJsonTree(message.getAttributes()).getAsJsonObject());
    data.addProperty("publishTimestamp", message.getPublishTime());
    data.addProperty("messageId", message.getMessageId());
    return gson.toJson(data);
  }

  private String getFilePath(String title) throws Exception {
    return digest(title).substring(0,10);
  }

  private String digest(String data) throws NoSuchAlgorithmException {
    StringBuilder hexString = new StringBuilder();
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    byte[] hash = md.digest(data.getBytes());

    for (byte aHash : hash) {
      if ((0xff & aHash) < 0x10) {
        hexString.append("0").append(Integer.toHexString((0xFF & aHash)));
      } else {
        hexString.append(Integer.toHexString(0xFF & aHash));
      }
    }
    return hexString.toString();
  }

  private byte[] decompress(byte[] bytesToDecompress) throws IOException {
    byte[] returnValues = null;

    Inflater inflater = new Inflater();
    int numberOfBytesToDecompress = bytesToDecompress.length;

    inflater.setInput(
      bytesToDecompress,
      0,
      numberOfBytesToDecompress
    );

    int bufferSizeInBytes = numberOfBytesToDecompress;

    List<Byte> bytesDecompressedSoFar = new ArrayList<Byte>();

    try {
      while (!inflater.needsInput()) {
        byte[] bytesDecompressedBuffer = new byte[bufferSizeInBytes];
        int numberOfBytesDecompressedThisTime = inflater.inflate(bytesDecompressedBuffer);
        for (int b = 0; b < numberOfBytesDecompressedThisTime; b++) {
          bytesDecompressedSoFar.add(bytesDecompressedBuffer[b]);
        }
      }

      returnValues = new byte[bytesDecompressedSoFar.size()];
      for (int b = 0; b < returnValues.length; b++){
        returnValues[b] = bytesDecompressedSoFar.get(b);
      }
    } catch (DataFormatException dfe) {
      throw new IOException();
    }

    inflater.end();
    return returnValues;
  }

  private String getFolderId(DatastoreService datastore, Drive service, String rootFid, String datatype, String datepath, String date) throws IOException, ParseException {
    String key = datatype + "/" + datepath;
    DriveHelper dh = DriveHelper.getInstance();
    Key searchKey = KeyFactory.createKey("DriveMapping", key);
    Query q = new Query("DriveMapping")
        .setFilter(new Query.FilterPredicate(
            Entity.KEY_RESERVED_PROPERTY,
            Query.FilterOperator.EQUAL,
            searchKey)
        );
    PreparedQuery pq = datastore.prepare(q);
    List<Entity> folders = pq.asList(FetchOptions.Builder.withLimit(1));

    String theFolderId;
    if (folders.size() == 1) {
      theFolderId = (String) folders.get(0).getProperty("folderId");
      System.out.println("I'MA HERE");
    } else {
      theFolderId = dh.createFolderByDate(datastore, service, rootFid, date, datatype);
    }
    System.out.println("Getted folderId: " + theFolderId);
    return theFolderId;
  }

}
