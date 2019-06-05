package com.isu.gdcb.wurtelelab.util;

import javax.swing.*;
import java.text.DecimalFormat;
import java.util.List;

/**
 * Created by nishanth and mhhur on 3/2/17.
 */
public class StringUtil {

    public static  String getSchemaString(List<String> colNames, String delimiter) {
        StringBuilder schemaString = new StringBuilder();
        for(String colName: colNames){
            schemaString.append(colName+delimiter);
        }
        return schemaString.toString();
    }

    public static double getThirdPrecisionValue(double val){
        DecimalFormat format = new DecimalFormat("##.000");
       /* try {
            val = Double.parseDouble(format.format(val));
        }catch(java.lang.NumberFormatException e){
            JOptionPane.showMessageDialog(null,"val:"+val);
        }*/
        //urmi
        if(Double.isNaN(val)){
            val=0.0;
        }
        val = Double.parseDouble(format.format(val));

        return val;
    }

    public static double getFifithPrecisionValue(double val){
        DecimalFormat format = new DecimalFormat("##.00000");
        val = Double.parseDouble(format.format(val));
        return val;
    }

    public static String buildFolderNameFromParams(Object... params){
        StringBuffer folderName = new StringBuffer();
        for(Object param: params){
            folderName.append(param.toString()+"_");
        }
        folderName = folderName.replace(folderName.lastIndexOf("_"), folderName.lastIndexOf("_") + 1, "");
        return folderName.toString();
    }

}

