package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLJoinCondition

class R2RMLRefObjectMap(val parentTriplesMapResource:Resource
    , val joinConditions:Set[R2RMLJoinCondition]) {
//	var owner:AbstractMappingDocument=null ;
//	var resource:Resource=null;
	
	val logger = Logger.getLogger(this.getClass().getName());
	var rdfNode:RDFNode = null;
	//private String alias;
	
	
	def getRelationName() = this.rdfNode.asResource().getLocalName();
	def getRangeClassMapping() = this.getParentTripleMapName;
	
//	public String getAlias() {
//		return alias;
//	}

	def getJoinConditions() : java.util.Collection[R2RMLJoinCondition] = {
		joinConditions;
	}

//	def getParentDatabaseColumnsString() : java.util.List[String] = {
//		val parentMap = this.getParentTriplesMap();
//		val result = parentMap.getSubjectReferencedColumns();
//		result;
//	}
//	
//	def getParentLogicalTable() : AbstractLogicalTable  = {
//		val result = try {
//			val triplesMap = this.getParentTriplesMap();
//			if(triplesMap != null) {
//				triplesMap.getLogicalTable();
//			}	else {
//			  null
//			}		
//		} catch {
//		  case e:Exception => {
//			val errorMessage = "Error while getting parent logical table!";
//			logger.warn(errorMessage);
//			null		    
//		  }
//		}
//
//		result;
//	}

	def getParentTripleMapName() :  String = {
		this.parentTriplesMapResource.getURI();
	}

//	def getParentTriplesMap() : AbstractConceptMapping  = {
//		val parentTriplesMapKey = this.parentTriplesMap.asResource().getLocalName();
//		val triplesMap = this.owner.getConceptMappingByMappingId(parentTriplesMapKey);
//		triplesMap;
//	}

	override def toString() : String = {
		this.parentTriplesMapResource.toString();
	}


}

object R2RMLRefObjectMap {
	val logger = Logger.getLogger(this.getClass().getName());
	
	def apply(resource:Resource) : R2RMLRefObjectMap = {
		val parentTriplesMapStatement = resource.getProperty(
		    Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
		
		val parentTriplesMap = Option(parentTriplesMapStatement).map(_.getObject.asResource).orNull
		
		val joinConditionsStatements = resource.listProperties(
		    Constants.R2RML_JOINCONDITION_PROPERTY);
		val joinConditions:Set[R2RMLJoinCondition] = if(joinConditionsStatements != null) {
		  joinConditionsStatements.map(joinConditionStatement => {
				R2RMLJoinCondition(joinConditionStatement.getObject.asResource);
		  }).toSet
		} else {
			val errorMessage = "No join conditions defined!";
			logger.warn(errorMessage);
			Set.empty;
		}
		
		val rom = new R2RMLRefObjectMap(parentTriplesMap, joinConditions);
//		rom.resource = resource;
		rom
	}
	
	def isRefObjectMap(resource:Resource ) : Boolean =
		resource.getProperty(Constants.R2RML_PARENTTRIPLESMAP_PROPERTY) != null
	
	def extractRefObjectMaps(resource:Resource) : Set[R2RMLRefObjectMap] = {
		val mapStatements = resource.listProperties(Constants.R2RML_OBJECTMAP_PROPERTY)
		mapStatements.flatMap(mapStatement => {
			val mapStatementObject = mapStatement.getObject()
			val mapStatementObjectResource = mapStatementObject.asResource
			if(R2RMLRefObjectMap.isRefObjectMap(mapStatementObjectResource)) {
				val rom = R2RMLRefObjectMap(mapStatementObjectResource)
				rom.rdfNode = mapStatementObjectResource
				Some(rom)
			} else {
				None
			}
		}).toSet
	}  
}