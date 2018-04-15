package in.feryand.extract;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.kohlschutter.boilerpipe.BoilerpipeProcessingException;
import com.kohlschutter.boilerpipe.extractors.ArticleExtractor;
import fi.iki.elonen.NanoHTTPD;

import javax.servlet.http.HttpServlet;

public class App extends NanoHTTPD {
  private final static Integer PORT = 8080;

  private final static Logger logger = Logger.getLogger(App.class.getName());

  private App() throws IOException {
      super(PORT);
      logger.info(String.format("Server running on port %d", PORT));
  }

  public static void main(String[] args) {
      try {
          App app = new App();
          app.setAsyncRunner(new BoundRunner(Executors.newFixedThreadPool(10)));
          app.start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
      } catch (IOException ioe) {
          logger.log(Level.SEVERE, "Couldn't start server", ioe);
      }
  }

  private Response messageResponse(Response.IStatus code, String message) {
    return newFixedLengthResponse(
        Response.Status.METHOD_NOT_ALLOWED,
        "application/json",
        String.format("{\"status\": \"failed\", \"message\":\"%s\"}", message)
    );
  }

  @Override
  public Response serve(IHTTPSession session) {
      Method method = session.getMethod();
      Map<String, String> body = new HashMap<String, String>();

      switch (method) {
          case POST:
              String html;
              try {
                  session.parseBody(body);
                  if (!session.getHeaders().get("content-type").equals("application/json")) {
                    throw new IOException();
                  }
                  if (!body.containsKey("postData")) {
                    throw new IOException();
                  }
                  JsonObject data = new JsonParser().parse(body.get("postData")).getAsJsonObject();
                  if (!data.has("html")) {
                    throw new IOException();
                  }
                  html = data.get("html").getAsString();
              } catch (Exception e) {
                  logger.log(Level.WARNING, "Request raise exception. Body: " + body, e);
                  return messageResponse(
                      Response.Status.BAD_REQUEST, "Houston, we've had an exception"
                  );
              }

              try {
                String data = ArticleExtractor.INSTANCE.getText(html);
                JsonStringEncoder encoder = JsonStringEncoder.getInstance();
                return newFixedLengthResponse(
                    Response.Status.OK,
                    "application/json",
                    String.format("{\"status\": \"success\", \"data\":\"%s\"}", new String(encoder.quoteAsString(data)))
                );
              } catch (BoilerpipeProcessingException e) {
                logger.log(Level.WARNING, "Boilerpipe raise exception. HTML: " + html.substring(0, 100), e);
                return messageResponse(
                    Response.Status.INTERNAL_ERROR, "Error processing HTML data"
                );
              }
          default:
            return messageResponse(
                Response.Status.METHOD_NOT_ALLOWED,
                "Method not allowed"
            );
      }
  }
}