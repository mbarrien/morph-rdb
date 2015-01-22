package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.Constants
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping

class R2RMLPredicateObjectMap(val predicateMaps:List[R2RMLPredicateMap]
		, val objectMaps:List[R2RMLObjectMap], val refObjectMaps:List[R2RMLRefObjectMap]
		//, objectMapTypes:List[R2RMLPredicateObjectMap.ObjectMapType.Value]
		, val graphMaps:Set[R2RMLGraphMap] ) extends MorphBasePropertyMapping 
{
	val logger = Logger.getLogger(this.getClass().getName());
	var alias:String = null;

//	def buildMetadata(dbMetadata:MorphDatabaseMetaData) = {
//	  try {
//		  if(this.predicateMaps != null) {
//			  this.predicateMaps.foreach(pm => { 
//			    if(pm != null) {pm.buildMetadata(dbMetadata)}});
//		  }
//		  
//		  if(this.objectMaps != null) {
//		    this.objectMaps.foreach(om => { 
//		      if(om != null) {om.buildMetadata(dbMetadata)} })
//		  }
//		  
//		  if(this.refObjectMaps != null) {
//		    
//		  }
//	  } catch {
//	    case e:Exception => {
//	      e.printStackTrace()
//	      logger.warn("Error building metadata for predicateobjectmap:" + this);
//	    }
//	  }
//	}
	
	def getMappedPredicateName(index:Int ) : String =
		this.predicateMaps.lift(index).map(_.getOriginalValue).orNull

	def getObjectMap(index:Int) : R2RMLObjectMap = this.objectMaps.lift(index).orNull

//	def getObjectMapType(index:Int) : R2RMLPredicateObjectMap.ObjectMapType.Value  = {
//		val result = if(this.objectMapTypes != null && !this.objectMapTypes.isEmpty()) {
//			this.objectMapTypes.get(index);
//		} else {
//			null;
//		}
//		result;
//	}

	def getPredicateMap(index:Int ) : R2RMLPredicateMap = this.predicateMaps.lift(index).orNull

	def getPropertyMappingID() : String  = {
		// TODO Auto-generated method stub
		null;
	}

//	override def getPropertyMappingType(index:Int ) : MappingType = {
//		val result = if(this.objectMaps != null && !this.objectMaps.isEmpty() 
//				&& this.objectMaps.get(index) != null) {
//			val objectMapTermType = this.objectMaps(index).inferTermType;
//			if(objectMapTermType.equals(Constants.R2RML_LITERAL_URI)) {
//				MappingType.ATTRIBUTE;
//			} else {
//				MappingType.RELATION;
//			}
//		} else if(this.refObjectMaps != null && !this.refObjectMaps.isEmpty() 
//				&& this.refObjectMaps.get(index)  != null) {
//			MappingType.RELATION;
//		} else {
//			null;
//		}
//		result;
//	}

	def getRangeClassMapping(index:Int ) : String =
		this.refObjectMaps.lift(index).map(_.getParentTripleMapName).orNull

	def getRefObjectMap(index:Int ) : R2RMLRefObjectMap = this.refObjectMaps.lift(index).orNull

	def getRelationName() : String = {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getRelationName");
		null;
	}

	override def toString() : String =
		"R2RMLPredicateObjectMap [predicateMaps=" + predicateMaps + ", objectMaps=" + objectMaps + ", refObjectMaps=" + refObjectMaps + "]";

	def getAlias() : String = alias

	def setAlias(alias:String ) = { this.alias = alias }

	override def getMappedPredicateNames() : Iterable[String] =
		this.predicateMaps.map(_.getOriginalValue)
  
	def getAttributeName() : String = {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getAttributeName");
		null;
	}	
}

object R2RMLPredicateObjectMap {
  	object ObjectMapType extends Enumeration {
		type ObjectMapType = Value
		val ObjectMap, RefObjectMap = Value
	}
  	
	def apply(resource:Resource) : R2RMLPredicateObjectMap = {
		val predicateMaps = R2RMLPredicateMap.extractPredicateMaps(resource).toList;
		//val tupleObjectMaps = R2RMLPredicateObjectMap.extractObjectMaps(resource);
		//val refObjectMaps = tupleObjectMaps.map(x => x._3);
		//val objectMaps = tupleObjectMaps.map(x => x._2);
		//val objectMapTypes = tupleObjectMaps.map(x => x._1);
		val objectMaps = R2RMLObjectMap.extractObjectMaps(resource).toList;
		val refObjectMaps = R2RMLRefObjectMap.extractRefObjectMaps(resource).toList;
		val graphMaps = R2RMLGraphMap.extractGraphMaps(resource);
//		val graphMap = if(graphMaps != null && !graphMaps.isEmpty) {
//			this.extractGraphMaps(resource, parent)(0);  
//		} else {
//			null;
//		}
		
		new R2RMLPredicateObjectMap(predicateMaps, objectMaps, refObjectMaps, graphMaps)
	}

	def extractPredicateObjectMaps(resource:Resource) : Set[R2RMLPredicateObjectMap] = {
		resource.listProperties(Constants.R2RML_PREDICATEOBJECTMAP_PROPERTY).map(statement =>
			R2RMLPredicateObjectMap(statement.getObject.asResource)
		).toSet
	}	
}