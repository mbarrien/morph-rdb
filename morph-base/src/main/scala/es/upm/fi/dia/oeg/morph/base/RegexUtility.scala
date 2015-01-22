package es.upm.fi.dia.oeg.morph.base

import scala.util.matching.Regex
import scala.collection.JavaConversions._
import java.util.regex.Matcher
import java.util.regex.Pattern

object RegexUtility {
	val patternString1 = Constants.R2RML_TEMPLATE_PATTERN;
	val TemplatePattern1 = patternString1.r;
  
	def main(args:Array[String]) = {
		val template = "Hello \\{ {Name} \\} Please find attached {Invoice Number} which is due on {Due Date}";
		
		val replacements = Map("Name" -> "Freddy", "Invoice Number" -> "INV0001")
		
		val attributes = RegexUtility.getTemplateColumns(template, true);
		System.out.println("attributes = " + attributes);
		
		val template2 = RegexUtility.replaceTokens(template, replacements);
		System.out.println("template2 = " + template2);	  
	}
	
	def getTemplateMatching(inputTemplateString: String, inputURIString : String) 
	: Map[String, String] = {
		val newTemplatePrefix = if (inputTemplateString.startsWith("<")) "" else "<"
		val newTemplateSuffix = if (inputTemplateString.startsWith(">")) "" else ">"
		val newTemplateString = newTemplatePrefix + inputTemplateString + newTemplateSuffix

		val newURIPrefix = if (inputURIString.startsWith("<")) "" else "<"
		val newURISuffix = if (inputURIString.startsWith(">")) "" else ">"
		val newURIString = newURIPrefix + inputURIString + newURISuffix
	  
		val columnsFromTemplate = this.getTemplateColumns(newTemplateString, false);
		//println("columnsFromTemplate = " + columnsFromTemplate);
		
		val columnsList = columnsFromTemplate.map(_.drop(1).dropRight(1))
		val templateString = columnsFromTemplate.foldLeft(newTemplateString)(
			(curTemplate, column) => curTemplate.replaceAll("\\{" + column + "\\}", "(.+?)"))
				
		val pattern = new Regex(templateString)
		val firstMatch = pattern.findFirstMatchIn(newURIString);
		
		firstMatch.map(matched => columnsList.zipWithIndex.map({
			case (column, i) => column -> matched.subgroups(i)
		}).toMap).getOrElse(Map.empty)
	}
	
	def getTemplateColumns(templateString0 : String, cleanColumn : Boolean) : java.util.List[String] = {
		val columnsFromTemplate = TemplatePattern1.findAllIn(templateString0).toList;
		if(cleanColumn) columnsFromTemplate.map(_.drop(1).dropRight(1)) else columnsFromTemplate
	}
	
	def replaceTokens(matcher:Matcher, text: String, replacements:java.util.Map[String, Object] ) 
	: String = {
		var buffer:StringBuffer = new StringBuffer();
		while (matcher.find()) {
			val matcherGroup1 = matcher.group(1);
			val replacementAux = replacements.get(matcherGroup1);;
			val replacement = if(replacementAux == null) { ""} 
			else { replacementAux }
			
			if (replacement != null) {
				matcher.appendReplacement(buffer, "");
				buffer.append(replacement);
			}
		}
		matcher.appendTail(buffer);
		return buffer.toString();		
	}
	
	def replaceTokens(pText:String, replacements:Map[String, Object] ) : String  = {
	  if(replacements != null && !replacements.isEmpty) {
		val text = pText.replaceAll("\\\\\\{", "morphopencurly")
				.replaceAll("\\\\\\}", "morphclosecurly");
		
		val pattern = Pattern.compile("\\{(.+?)\\}");
		val matcher = pattern.matcher(text);
		
		val replacedToken = RegexUtility.replaceTokens(matcher, text, replacements)
			.replaceAll("morphopencurly", "\\{")
			.replaceAll("morphclosecurly", "\\}");

		return replacedToken;	    
	  } else { null }

	}	
}