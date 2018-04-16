package in.feryand.extract.servlet;

import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.cloud.storage.*;
import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;

import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.kohlschutter.boilerpipe.BoilerpipeProcessingException;
import com.kohlschutter.boilerpipe.extractors.ArticleExtractor;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
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

  private static final String BUCKET = "actadiurna";
  private static final String BACKUP_BUCKET = "actadiurna-data";
  private static final String BASE = "articles/";

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
      String filename = getFilePath(attributes.get("date"), attributes.get("title"));

      createFile(
          BACKUP_BUCKET,
          "json/" + filename + ".message.json",
          pubsubToJsonString(message).getBytes()
      );

      createFile(
          BUCKET,
          BASE + "html/" + filename + ".html",
          html.getBytes()
      );

      createFile(
          BUCKET,
          BASE + "json/" + filename + ".json",
          articleToJsonString(
              attributes.get("url"),
              attributes.get("date"),
              attributes.get("referrer"),
              attributes.get("title"),
              article,
              filename
          ).getBytes()
      );
    } catch (Exception e) {
      e.printStackTrace();
      throw new IOException();
    }

    deleteMemcache();

    resp.setStatus(HttpServletResponse.SC_OK);
    resp.getWriter().close();
  }

  private void createFile(String bucket, String path, byte[] data) {
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
      String filepath) {
    Gson gson = new Gson();
    JsonObject data = new JsonObject();
    data.addProperty("url", url);
    data.addProperty("date", date);
    data.addProperty("referrer", referrer);
    data.addProperty("title", title);
    data.addProperty("content", content);
    data.addProperty("html", "https://storage.googleapis.com/actadiurna/" + BASE + "html/" + filepath + ".html");
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

  private String getFilePath(String date, String title) throws Exception {
    return getDatePath(date) + digest(title).substring(0,10);
  }

  private String getDatePath(String dateString) throws ParseException {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date date = df.parse(dateString);
    Calendar calendar = new GregorianCalendar();
    calendar.setTime(date);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH) + 1;
    int day = calendar.get(Calendar.DAY_OF_MONTH);

    return year + "/" + month + "/" + day + "/";
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

  private void deleteMemcache() {
    MemcacheService memcacheService =
        MemcacheServiceFactory.getMemcacheService();
    memcacheService.delete("messageCache");
  }

}
