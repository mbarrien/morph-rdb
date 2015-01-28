package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._

import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.rdf.model.RDFNode

class R2RMLPredicateMap(termMapType:Constants.MorphTermMapType.Value
    , termType:Option[String], datatype:Option[String], languageTag:Option[String]) 
    extends R2RMLTermMap(termMapType, termType, datatype, languageTag) {

	val inferredTermType = this.inferTermType;
	if (!inferredTermType.equals(Constants.R2RML_IRI_URI)) {
		throw new Exception("Non IRI value is not permitted in the graph!");
	}  
}

object R2RMLPredicateMap {
	def apply(rdfNode:RDFNode) : R2RMLPredicateMap = {
		val coreProperties = R2RMLTermMap.extractCoreProperties(rdfNode);
		//coreProperties = (termMapType, termType, datatype, languageTag)
		val termMapType = coreProperties._1;
		val termType = coreProperties._2;
		val datatype = coreProperties._3;
		val languageTag = coreProperties._4;
		val pm =new R2RMLPredicateMap(termMapType, termType, datatype, languageTag);
		pm.parse(rdfNode);
		pm;
	}
	
	def extractPredicateMaps(resource:Resource) : Set[R2RMLPredicateMap] =
		R2RMLTermMap.extractTermMaps(resource, Constants.MorphPOS.pre).map(
			_.asInstanceOf[R2RMLPredicateMap])
}
