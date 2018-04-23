package in.feryand.extract.servlet;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.cloud.storage.*;
import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;

import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.kohlschutter.boilerpipe.extractors.ArticleExtractor;
import in.feryand.extract.objects.Pair;
import in.feryand.extract.objects.SimpleDate;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.*;

@SuppressWarnings("serial")
public class ReceiverServlet extends HttpServlet {
  private Storage storage = StorageOptions.getDefaultInstance().getService();

  /* Google Drive */
  private static final String APPLICATION_NAME = "Hoax Analyzer Project: Artikel";
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final String CREDENTIALS_FOLDER = "credentials";

  /* Google Drive Credentials */
  private static final List<String> SCOPES = Collections.singletonList(DriveScopes.DRIVE);

  private static final String rootFolderId = "12KdmXUxssdL5nKYz5afolHqHEW_fdCFI";
  private static final String jsonRootFolderId = "1y0MNe60KyP71aeP_eAbFJhmjeT4CUVnB";
  private static final String htmlRootFolderId = "1CwgMxDMnm53pUClG8XKvLOqJ1ZMCv-j8";

  /* Google Cloud Storage */
  private static final String BUCKET = "actadiurna";
  private static final String BACKUP_BUCKET = "actadiurna-data";
  private static final String BASE = "articles/";

  private static Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws IOException, GeneralSecurityException {
    java.io.File keyFile = new java.io.File("WEB-INF/3db05e8f216e.p12");
    return new GoogleCredential.Builder()
        .setTransport(HTTP_TRANSPORT)
        .setJsonFactory(JSON_FACTORY)
        .setServiceAccountId("hoax-analyzer-diurna@appspot.gserviceaccount.com")
        .setClientSecrets("108137694476666090803", "notasecret")
        .setServiceAccountScopes(DriveScopes.all())
        .setServiceAccountPrivateKeyFromP12File(keyFile)
        .build();
  }

  @Override
  @SuppressWarnings("unchecked")
  public final void doPost(final HttpServletRequest req,
                           final HttpServletResponse resp)
      throws IOException {

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

    byte[] decoded = Base64.getDecoder().decode(message.decodeData());

    try {
      String html = new String(decompress(decoded));
      String article = ArticleExtractor.INSTANCE.getText(html);
      String filename = getFilePath(attributes.get("title"));
      String sfilename = getDatePath(attributes.get("date")) + filename;

      /* BACKUP PUBSUB DATA */
      createGcsFile(
          BACKUP_BUCKET,
          "json/" + sfilename + ".message.json",
          pubsubToJsonString(message).getBytes()
      );

      /* HTML */
      createGcsFile(
          BUCKET,
          BASE + "html/" + sfilename + ".html",
          html.getBytes()
      );

      /* JSON */
      createGcsFile(
          BUCKET,
          BASE + "json/" + sfilename + ".json",
          articleToJsonString(
              attributes.get("url"),
              attributes.get("date"),
              attributes.get("referrer"),
              attributes.get("title"),
              article,
              filename,
              true
          ).getBytes()
      );

      /* GOOGLE DRIVE */

      final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      Drive service = new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, getCredentials(HTTP_TRANSPORT))
          .setApplicationName(APPLICATION_NAME)
          .build();

      /* GOOGLE DRIVE - HTML */
      uploadFileToDrive(
          service,
          htmlRootFolderId,
          filename,
          ".html",
          attributes.get("date"),
          html
      );

      /* GOOGLE DRIVE - JSON */
      uploadFileToDrive(
          service,
          jsonRootFolderId,
          filename,
          ".json",
          attributes.get("date"),
          articleToJsonString(
              attributes.get("url"),
              attributes.get("date"),
              attributes.get("referrer"),
              attributes.get("title"),
              article,
              filename,
              false
          )
      );

    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException();
    }

    deleteMemcache();

    resp.setStatus(HttpServletResponse.SC_OK);
    resp.getWriter().close();
  }

  private static SimpleDate getDate(String dateString) throws ParseException {
    SimpleDate result = new SimpleDate();
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date date = df.parse(dateString);
    Calendar calendar = new GregorianCalendar();
    calendar.setTime(date);

    result.year = Integer.toString(calendar.get(Calendar.YEAR));
    result.month = String.format("%02d", calendar.get(Calendar.MONTH) + 1);
    result.day = String.format("%02d", calendar.get(Calendar.DAY_OF_MONTH));

    return result;
  }

  private void createGcsFile(String bucket, String path, byte[] data) {
    BlobId blobId = BlobId.of(bucket, path);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    storage.create(blobInfo, data);
  }

  private static void uploadFileToDrive(Drive service, String rootFolderId, String filename, String ext, String dateString, String content) throws IOException, ParseException {
    String folderId = createFolderByDate(service, rootFolderId, dateString);
    File fileMetadata = new File();
    fileMetadata.setName(filename + ext)
        .setParents(Collections.singletonList(folderId));

    java.io.File temp =  java.io.File.createTempFile(filename, ext);
    BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
    bw.write(content);
    bw.close();

    FileContent mediaContent = new FileContent("text/plain", temp);

    File file = service.files()
        .create(fileMetadata, mediaContent)
        .setFields("id")
        .execute();
    temp.delete();
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

  private String getDatePath(String dateString) throws ParseException {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date date = df.parse(dateString);
    Calendar calendar = new GregorianCalendar();
    calendar.setTime(date);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH) + 1;
    int day = calendar.get(Calendar.DAY_OF_MONTH);

    return year + "/" +
        String.format("%02d", month) + "/" +
        String.format("%02d", day) + "/" +
        calendar.getTimeInMillis() + ".";
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

  private static String getFolderIdByName(Drive service, String searchFolderId, String name) throws IOException {
    FileList result = service.files().list()
        .setPageSize(40)
        .setFields("nextPageToken, files(id, name)")
        .setQ("'" + searchFolderId + "' in parents and trashed = false")
        .execute();
    List<File> files = result.getFiles();
    if (files == null || files.isEmpty()) {
      return null;
    } else {
      for (File f : files) {
        if (f.getName().equalsIgnoreCase(name)) {
          return f.getId();
        }
      }
    }
    return null;
  }

  private static Pair<String, String> getFolderIdByDate(Drive service, String rootFolderId, String dateString) throws ParseException, IOException {
    SimpleDate date = getDate(dateString);

    String yearFolder = getFolderIdByName(service, rootFolderId, date.year);

    if (yearFolder == null) {
      return new Pair<>("-", "");
    } else {
      String monthFolder = getFolderIdByName(service, yearFolder, date.month);

      if (monthFolder == null) {
        return new Pair<>("Y", yearFolder);
      } else {
        String dayFolder = getFolderIdByName(service, monthFolder, date.day);

        if (dayFolder == null) {
          return new Pair<>("M", monthFolder);
        }

        return new Pair<>("D", dayFolder);
      }
    }
  }

  private static String createFolder(Drive service, String parentFolderId, String folderName) throws IOException {
    File fileMetadata = new File();
    fileMetadata.setName(folderName)
        .setMimeType("application/vnd.google-apps.folder")
        .setParents(Collections.singletonList(parentFolderId));

    File file = service.files()
        .create(fileMetadata)
        .setFields("id")
        .execute();
    return file.getId();
  }

  private static String createFolderByDate(Drive service, String rootFolderId, String dateString) throws IOException, ParseException {
    Pair<String, String> checkResult = getFolderIdByDate(service, rootFolderId, dateString);
    SimpleDate date = getDate(dateString);

    String lastFolderId = rootFolderId;
    if (!checkResult.x.equals("-")) {
      lastFolderId = checkResult.y;
    }

    switch (checkResult.x) {
      case "-":
        lastFolderId = createFolder(service, lastFolderId, date.year);
      case "Y":
        lastFolderId = createFolder(service, lastFolderId, date.month);
      case "M":
        lastFolderId = createFolder(service, lastFolderId, date.day);
        checkResult.y = lastFolderId;
        break;
    }
    return checkResult.y;
  }

  private void deleteMemcache() {
    MemcacheService memcacheService =
        MemcacheServiceFactory.getMemcacheService();
    memcacheService.delete("messageCache");
  }

}
