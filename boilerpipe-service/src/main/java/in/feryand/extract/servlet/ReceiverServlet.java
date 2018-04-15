package in.feryand.extract.servlet;

import com.google.appengine.api.memcache.MemcacheServiceFactory;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.cloud.storage.*;
import com.google.api.client.json.JsonParser;
import com.google.api.client.json.jackson2.JacksonFactory;

import com.google.api.services.pubsub.model.PubsubMessage;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.zip.*;

@SuppressWarnings("serial")
public class ReceiverServlet extends HttpServlet {
  private Storage storage = StorageOptions.getDefaultInstance().getService();

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

    BlobId blobId = BlobId.of("actadiurna", attributes.get("title") + "-decompress");
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    storage.create(blobInfo, decompress(decoded));

    blobId = BlobId.of("actadiurna", attributes.get("title") + "-b64");
    blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
    storage.create(blobInfo, message.decodeData());

    MemcacheService memcacheService =
        MemcacheServiceFactory.getMemcacheService();
    memcacheService.delete("messageCache");

    resp.setStatus(HttpServletResponse.SC_OK);
    resp.getWriter().close();
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

}
