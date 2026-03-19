package bg.tid.iec104.service;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import java.util.regex.Matcher;

public class IecUtils {

    public static List<String> extractVariableNames(String equation) {
        List<String> variableNames = new ArrayList<>();
        
        // Regular expression pattern to match variable names
        Pattern pattern = Pattern.compile("([A-Za-z]+\\w*)");
        Matcher matcher = pattern.matcher(equation);
        
        // Find and add variable names to the list
        while (matcher.find()) {
            variableNames.add(matcher.group());
        }
        
        return variableNames;
    }
}
