package com.google.refine.extension.tachyon;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.time.FastDateFormat;
import org.json.JSONObject;

import tachyon.TachyonURI;
import tachyon.client.WriteType;


import com.google.refine.exporters.TabularSerializer;

final class TachyonSerializer implements TabularSerializer {
    String tableName;
    List<Exception> exceptions;
    
    String tableId;
    List<String> columnNames;
    StringBuffer sb;
    int rows;
    
    TachyonSerializer(String tableName, List<Exception> exceptions) {
        this.tableName = tableName;
        this.exceptions = exceptions;
    }
    
    @Override
    public void startFile(JSONObject options) {
            sb = new StringBuffer();
    }

    @Override
    public void endFile() {
        if (sb == null) 
            return;
        
        ByteBuffer buf = ByteBuffer.allocate(sb.length());
//            buf.put(sb.toString().getBytes("UTF-8"));
        buf.put(sb.toString().getBytes());
//        String masterURL = "tachyon://" + System.getenv("TACHYON_MASTER_ADDRESS") + ":19998";                 // "tachyon://208.75.74.214:19998"
        String masterURL = "tachyon://" + System.getProperty("TACHYON_MASTER_ADDRESS") + ":19998";
        Utils.runExample(new WriteOperation(new TachyonURI(masterURL), 
                new TachyonURI("/OpenRefine" + getCurrentTimeStamp()),
                WriteType.valueOf("MUST_CACHE"),
                buf));
    }

    
    private String getCurrentTimeStamp() {
        Date myDate = new Date();
        FastDateFormat fdf = FastDateFormat.getInstance("yyyy-MM-dd-HH-mm-ss");
        return fdf.format(myDate);
    }
    
    @Override
    public void addRow(List<CellData> cells, boolean isHeader) {
        if (isHeader) {
            columnNames = new ArrayList<String>(cells.size());
            for (CellData cellData : cells) {
                columnNames.add(cellData.text);
            }                
            try {
                boolean first = true;
                for (String columnName : columnNames) {            
                    if (first) {
                        first = false;
                    } else {
                        sb.append(',');
                    }
                    sb.append("'");
                    sb.append(columnName);
                    sb.append("'");
                    sb.append("\n");
                }
            } catch (Exception e) {
                tableId = null;
                exceptions.add(e);
            }
        } else  {
            formatCsv(cells, sb);            
            rows++;
        }
    }
    

    private void formatCsv(List<CellData> cells, StringBuffer sb) {
       boolean first = true;
        for (int i = 0; i < cells.size() && i < columnNames.size(); i++) {
            CellData cellData = cells.get(i);
            if (!first) {
                sb.append(',');
            } else {
                first = false;
            }
            sb.append("\"");
            if (cellData != null && cellData.text != null) {
                sb.append(cellData.text.replaceAll("\"", "\"\""));
            }
            sb.append("\"");
        }
        sb.append("\n");
    }
    
    public String getUrl() {
        return tableId == null || exceptions.size() > 0 ? null :
            "https://www.google.com/fusiontables/DataSource?docid=" + tableId;
    }
    
    public static void main(String args[]) {
        StringBuffer sb = new StringBuffer();
        sb.append("012345678");
        if (sb == null) 
            return;     
        
        ByteBuffer buf = ByteBuffer.allocate(sb.length());
        try {
            buf.put(sb.toString().getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        buf.flip();
        System.out.println(buf.getChar(8));
    }
}
