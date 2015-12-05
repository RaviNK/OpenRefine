package com.google.refine.extension.tachyon;

import java.io.IOException;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.refine.ProjectManager;
import com.google.refine.browsing.Engine;
import com.google.refine.commands.Command;
import com.google.refine.commands.project.ExportRowsCommand;
import com.google.refine.exporters.CustomizableTabularExporterUtilities;
import com.google.refine.model.Project;

public class UploadCommand extends Command {
    static final Logger logger = LoggerFactory.getLogger("tachyon_upload");
    private static final String SPREADSHEET_FEED = "https://spreadsheets.google.com/feeds/spreadsheets/private/full";
    
    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        
        ProjectManager.singleton.setBusy(true);
        try {
            Project project = getProject(request);
            Engine engine = getEngine(request, project);
            Properties params = ExportRowsCommand.getRequestParameters(request);
            String name = params.getProperty("name");
            
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Content-Type", "application/json");
            
            Writer w = response.getWriter();
            JSONWriter writer = new JSONWriter(w);
            try {
                writer.object();
                
                List<Exception> exceptions = new LinkedList<Exception>();
                upload(project, engine, params, name, exceptions);
                if (exceptions.size() > 0 ) {
                    for (Exception e : exceptions) {
                        logger.warn(e.getLocalizedMessage(), e);
                    }
                    writer.key("status"); writer.value("error");
                    writer.key("message"); writer.value(exceptions.get(0).getLocalizedMessage());
                }
            } catch (Exception e) {
                e.printStackTrace();
                writer.key("status"); writer.value("error");
                writer.key("message"); writer.value(e.getMessage());
            } finally {
                writer.endObject();
                w.flush();
                w.close();
            }
        } catch (Exception e) {
            throw new ServletException(e);
        } finally {
            ProjectManager.singleton.setBusy(false);
        }
    }
    
    static private void upload(
            Project project, Engine engine, Properties params,
           String name, List<Exception> exceptions) {
        uploadToTachyon(project, engine, params, name, exceptions);
    }
    
    
    // TODO: return upload results 
    static private void uploadToTachyon(
            Project project, final Engine engine, final Properties params,
            String name, List<Exception> exceptions) {
        
       TachyonSerializer serializer = new TachyonSerializer(name, exceptions);
        
        CustomizableTabularExporterUtilities.exportRows(
                project, engine, params, serializer);
        
    }
}
