package com.predictspring.dataingestor.DataLoader;

import java.util.Collections;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;

import java.util.StringJoiner;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;


public class Product {

    // private static final Logger logger = LoggerFactory.getLogger(Product.class);

    private static final Set<String> ColunmNamesSet;
    private static final Map<String, String> ColumnNamesMapping;

    public enum ColumnNames {
        /*LASTUPDATE*/
        PRODUCTID, NAME, DESCRIPTION, PRODUCTURL, IMAGEURL,
        ADDITIONALIMAGEURL, PRODUCTCONDITION, PRICE, BRAND,
        MANUFACTURER, MANUFACTURERDETAILS, MANUFACTURERLOCATION,
        GTIN, MPN, GENDER, COLOR, SIZE, MATERIAL, SHIPPING,
        SHIPPINGWEIGHT, MERCHANTSKU, CHANNELAPPLICABILITY,
        AGEGROUP, AVAILABILITY, CURRENCYCODE, PRODUCTLOCATION
    }

    public enum ChannelApplicability {
        ALL, ONLINE, INSTORE, OUTLET, MAIL // Possible channels where the product can be applicable
    }

    public enum ProductCondition {
        NEW, USED, REFURBISHED // Possible conditions of the product
    }

    public enum ProductLocation {
        WEB, STORE, ALL // Possible locations where the product is available
    }

    // NOTE: We can use reflection once the scale grows
    static {
        Set<String> aSet = new HashSet<String>();
        for (ColumnNames location : ColumnNames.values()) {
            aSet.add(location.name());
        }
        ColunmNamesSet = Collections.unmodifiableSet(aSet);

        HashMap<String, String> aMap = new HashMap<String, String>();
        aMap.put("sku_id", ColumnNames.PRODUCTID.name());
        aMap.put("image", ColumnNames.IMAGEURL.name());
        aMap.put("additional_image_link", ColumnNames.ADDITIONALIMAGEURL.name());
        aMap.put("vendor", ColumnNames.MERCHANTSKU.name());
        aMap.put("list_price", ColumnNames.PRICE.name());
        aMap.put("currency", ColumnNames.CURRENCYCODE.name());
        aMap.put("title", ColumnNames.NAME.name());
        aMap.put("link", ColumnNames.PRODUCTURL.name());
        aMap.put("prod_description", ColumnNames.DESCRIPTION.name());
        ColumnNamesMapping = Collections.unmodifiableMap(aMap);
    }


    public static String generateInsertSQL(String[] columnNames) {
        StringJoiner columns = new StringJoiner(",");
        StringJoiner placeholders = new StringJoiner(",");
        StringJoiner updateStatement = new StringJoiner(",");

        for (String column : columnNames) {
            
            if (column == null) {
                continue;
            }
            
            String upperCaseColumn = column.toUpperCase(); 

            columns.add(upperCaseColumn);
            placeholders.add("?");
            
            updateStatement.add(upperCaseColumn + "=" + "?");
        }
        
        if (columns.length() == 0) {
            return null;
        }
        
        return "INSERT INTO" + " Product(" + columns.toString() + 
        ") VALUES(" + placeholders.toString() + ")" +
        " ON DUPLICATE KEY UPDATE " + updateStatement.toString();
    }


    public static boolean contains(String key) {
        return ColunmNamesSet.contains(key.toUpperCase());
    }

    public static boolean mappingExist(String key) {
        return ColumnNamesMapping.containsKey(key);
    }

    public static String getMapping(String key) {
        return ColumnNamesMapping.get(key);
    }
}