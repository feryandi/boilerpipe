package in.feryand.extract.servlet;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.kohlschutter.boilerpipe.BoilerpipeProcessingException;
import com.kohlschutter.boilerpipe.extractors.ArticleExtractor;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.stream.Collectors;

@SuppressWarnings("serial")
public class AppServlet extends HttpServlet {

  @Override
  public void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
      JsonObject response = new JsonObject();
      response.add("status", new JsonPrimitive("failed"));
      response.add("message", new JsonPrimitive("Method not allowed"));

      resp.setHeader("Content-Type", "application/json");
      resp.setStatus(405);
      PrintWriter out = resp.getWriter();
      out.println(response.toString());
  }

  @Override
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    if (!req.getHeader("Content-Type").equals("application/json")) {
      throw new IOException();
    }

    String postData = req.getReader().lines().collect(Collectors.joining(System.lineSeparator()));
    JsonObject data = new JsonParser().parse(postData).getAsJsonObject();

    if (!data.has("html")) {
      throw new IOException();
    }

    String html = data.get("html").getAsString();

    resp.setHeader("Content-Type", "application/json");
    PrintWriter out = resp.getWriter();
    JsonObject response = new JsonObject();

    try {
      String article = ArticleExtractor.INSTANCE.getText(html);
      JsonStringEncoder encoder = JsonStringEncoder.getInstance();

      response.add("status", new JsonPrimitive("success"));
      response.add("data", new JsonPrimitive(new String(encoder.quoteAsString(article))));
      resp.setStatus(200);
    } catch (BoilerpipeProcessingException e) {
      response.add("status", new JsonPrimitive("success"));
      response.add("data", new JsonPrimitive("Method not allowed"));
      resp.setStatus(500);
    }

    out.println(response.toString());
  }
}
