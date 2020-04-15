package com.spark.common;

public class CommonUtils {
    public static String fulfuill(String str) {
        if(str.length() == 1)
            return "0" + str;
        return str;
    }

    public static String fulfuill(int num,String str) {
        if(str.length() == num) {
            return str;
        } else {
            int fulNum = num-str.length();
            String tmpStr  =  "";
            for(int i = 0; i < fulNum ; i++){
                tmpStr += "0";
            }
            return tmpStr + str;
        }
    }

    /**
     * 从拼接的字符串中提取字段
     * @param str 字符串
     * @param delimiter 分隔符
     * @param field 字段
     * @return 字段值
     * name=zhangsan|age=18
     */
    public static String getFieldFromConcatString(String str,String delimiter, String field) {
        try {
            String[] fields = str.split(delimiter);
            for(String concatField : fields) {
                // searchKeywords=|clickCategoryIds=1,2,3
                if(concatField.split("=").length == 2) {
                    String fieldName = concatField.split("=")[0];
                    String fieldValue = concatField.split("=")[1];
                    if(fieldName.equals(field)) {
                        return fieldValue;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 从拼接的字符串中给字段设置值
     * @param str 字符串
     * @param delimiter 分隔符
     * @param field 字段名
     * @param newFieldValue 新的field值
     * @return 字段值
     *  name=zhangsan|age=12
     *  |
     *  age
     *  18
     *  name=zhangsan|age=18
     */
    public static String setFieldInConcatString(String str,
                                                String delimiter,
                                                String field,
                                                String newFieldValue) {

        // searchKeywords=iphone7|clickCategoryIds=1,2,3

        String[] fields = str.split(delimiter);

        for(int i = 0; i < fields.length; i++) {
            String fieldName = fields[i].split("=")[0];
            if(fieldName.equals(field)) {
                String concatField = fieldName + "=" + newFieldValue;
                fields[i] = concatField;
                break;
            }
        }

        StringBuffer buffer = new StringBuffer("");
        for(int i = 0; i < fields.length; i++) {
            buffer.append(fields[i]);
            if(i < fields.length - 1) {
                buffer.append("|");
            }
        }

        return buffer.toString();
    }

}
