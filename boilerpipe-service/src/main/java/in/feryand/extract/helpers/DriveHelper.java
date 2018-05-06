package in.feryand.extract.helpers;

import com.google.api.client.http.FileContent;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;
import com.google.appengine.api.datastore.*;
import in.feryand.extract.objects.Pair;
import in.feryand.extract.objects.SimpleDate;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;

public class DriveHelper {
  private static TimeHelper th = TimeHelper.getInstance();

  private static DriveHelper singleton = new DriveHelper();

  private DriveHelper() { }

  public static DriveHelper getInstance( ) {
    return singleton;
  }

  public String uploadFileToDrive(Drive service, String folderId, String filename, String ext, String content) throws IOException, ParseException {
    File fileMetadata = new File();
    fileMetadata.setName(filename + ext)
        .setParents(Collections.singletonList(folderId));

    java.io.File temp = java.io.File.createTempFile(filename, ext);
    BufferedWriter bw = new BufferedWriter(new FileWriter(temp));
    bw.write(content);
    bw.close();

    FileContent mediaContent = new FileContent("text/plain", temp);

    File file = service.files()
        .create(fileMetadata, mediaContent)
        .setFields("id")
        .execute();
    temp.delete();
    return file.getId();
  }

  private String getKeyValue(DatastoreService datastore, String key) {
    Key searchKey = KeyFactory.createKey("DriveMapping", key);
    Query q = new Query("DriveMapping")
        .setFilter(new Query.FilterPredicate(
            Entity.KEY_RESERVED_PROPERTY,
            Query.FilterOperator.EQUAL,
            searchKey)
        );
    PreparedQuery pq = datastore.prepare(q);
    List<Entity> folders = pq.asList(FetchOptions.Builder.withLimit(1));

    if (folders.size() == 1) {
      return (String) folders.get(0).getProperty("folderId");
    }
    return null;
  }

  private String getFolderIdByName(DatastoreService datastore, Drive service, String searchFolderId, String name, String path) throws IOException {
    String savedPath = getKeyValue(datastore, path);
    if (savedPath != null) {
      return savedPath;
    }
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
          datastore.put(
              createEntity(path, f.getId())
          );
          return f.getId();
        }
      }
    }
    return null;
  }

  private Pair<String, String> getFolderIdByDate(DatastoreService datastore, Drive service, String rootFolderId, String datatype, String dateString) throws ParseException, IOException {
    SimpleDate date = th.getDate(dateString);
    String path = datatype + "/" + date.year;
    String yearFolder = getFolderIdByName(datastore, service, rootFolderId, date.year, path);

    if (yearFolder == null) {
      return new Pair<>("-", "");
    } else {
      path += "/" + date.month;
      String monthFolder = getFolderIdByName(datastore, service, yearFolder, date.month, path);

      if (monthFolder == null) {
        return new Pair<>("Y", yearFolder);
      } else {
        path += "/" + date.day;
        String dayFolder = getFolderIdByName(datastore, service, monthFolder, date.day, path);

        if (dayFolder == null) {
          return new Pair<>("M", monthFolder);
        } else {
          path += "/" + date.hour;
          String hourFolder = getFolderIdByName(datastore, service, dayFolder, date.hour, path);

          if (hourFolder == null) {
            return new Pair<>("D", dayFolder);
          }
          return new Pair<>("H", hourFolder);
        }
      }
    }
  }

  public String createFolder(Drive service, String parentFolderId, String folderName) throws IOException {
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

  private Entity createEntity(String key, String value) {
    Entity data = new Entity("DriveMapping", key);
    data.setProperty("folderId", value);
    return data;
  }

  public String createFolderByDate(DatastoreService datastore, Drive service, String rootFolderId, String dateString, String datatype) throws IOException, ParseException {
    Pair<String, String> checkResult = getFolderIdByDate(datastore, service, rootFolderId, datatype, dateString);
    SimpleDate date = th.getDate(dateString);

    String lastFolderId = rootFolderId;
    if (!checkResult.x.equals("-")) {
      lastFolderId = checkResult.y;
    }

    switch (checkResult.x) {
      case "-":
        lastFolderId = createFolder(service, lastFolderId, date.year);
        datastore.put(
            createEntity(datatype + "/" + date.year, lastFolderId)
        );
      case "Y":
        lastFolderId = createFolder(service, lastFolderId, date.month);
        datastore.put(
            createEntity(datatype + "/" + date.year + "/" + date.month, lastFolderId)
        );
      case "M":
        lastFolderId = createFolder(service, lastFolderId, date.day);
        datastore.put(
            createEntity(datatype + "/" + date.year + "/" + date.month + "/" + date.day, lastFolderId)
        );
      case "D":
        lastFolderId = createFolder(service, lastFolderId, date.hour);
        datastore.put(
            createEntity(datatype + "/" + date.year + "/" + date.month + "/" + date.day + "/" + date.hour, lastFolderId)
        );
        checkResult.y = lastFolderId;
        break;
    }
    return checkResult.y;
  }
}
