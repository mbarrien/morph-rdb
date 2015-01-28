package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import com.hp.hpl.jena.rdf.model.Resource
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import java.sql.DatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseLogicalTable
import es.upm.fi.dia.oeg.morph.base.model.IConceptMapping
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElement
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLLogicalTable

class R2RMLTriplesMap(val logicalTable:R2RMLLogicalTable, val subjectMap:R2RMLSubjectMap
    , val predicateObjectMaps:Set[R2RMLPredicateObjectMap]) 
extends MorphBaseClassMapping(predicateObjectMaps) with MorphR2RMLElement with IConceptMapping
{
	
  	val logger = Logger.getLogger(this.getClass());
	//var triplesMapName:String = null;
	
	def buildMetaData(dbMetadata:Option[MorphDatabaseMetaData]) = {
	  logger.debug("Building metadata for TriplesMap: " + this.name);
	  
	  this.logicalTable.buildMetaData(dbMetadata);
//	  this.subjectMap.buildMetadata(dbMetadata);
//	  this.predicateObjectMaps.foreach(pom => pom.buildMetadata(dbMetadata));
	}
	
	def accept(visitor:MorphR2RMLElementVisitor ) : Object = visitor.visit(this)
	
	override def toString() : String = this.name
	
	override def getConceptName() :String = {
		val classURIs = this.subjectMap.classURIs;
		if (classURIs.isEmpty) {
			logger.warn("No class URI defined for TriplesMap: " + this);
			null
		} else {
			if(classURIs.size > 1) {
				logger.warn("Multiple classURIs defined, only one is returned!");
			}
			classURIs.head
		}
	}	

	override def getPropertyMappings(propertyURI:String ) : Iterable[MorphBasePropertyMapping] =
		this.predicateObjectMaps.filter(_.getMappedPredicateName(0).equals(propertyURI))
	
	override def getPropertyMappings() : Iterable[MorphBasePropertyMapping] =
		this.predicateObjectMaps
	
//	override def getRelationMappings() : java.util.Collection[IRelationMapping] = {
//		val result = if(this.predicateObjectMaps != null) {
//			this.predicateObjectMaps.flatMap(pm => {
//				val mappingType = pm.getPropertyMappingType(0);
//				if(mappingType == MappingType.RELATION) {
//					Some(pm);
//				} else {
//				  None
//				}
//			});
//		} else {
//		  Nil
//		}
//		result;
//	}
	
	override def isPossibleInstance(uri:String ) : Boolean = {
		if(this.subjectMap.termMapType == Constants.MorphTermMapType.TemplateTermMap) {
			val templateValues = this.subjectMap.getTemplateValues(uri)
			templateValues.nonEmpty && !templateValues.values.exists(_.contains("/"))
		} else {
			val errorMessage = "Can't determine whether " + uri + " is a possible instance of " + this.toString();
			logger.warn(errorMessage);
			false
		}
	}
	
	override def getLogicalTableSize() : Long =
		this.logicalTable.getLogicalTableSize()

	override def getMappedClassURIs() : Iterable[String] =
		this.subjectMap.classURIs;

	override def getTableMetaData() : Option[MorphTableMetaData] =
		this.logicalTable.tableMetaData
	
	def getLogicalTable(): R2RMLLogicalTable =
	  this.logicalTable

	override def getSubjectReferencedColumns() : List[String] =
	  this.subjectMap.getReferencedColumns()

}

object R2RMLTriplesMap {
	val logger = Logger.getLogger(this.getClass().getName());
	
	def apply(tmResource:Resource) : R2RMLTriplesMap = {
		
		//LOGICAL TABLE
		val logicalTableStatement = tmResource.getProperty(Constants.R2RML_LOGICALTABLE_PROPERTY);
		if(logicalTableStatement == null) {
			val  errorMessage = "Missing rr:logicalTable";
			logger.error(errorMessage);
			throw new Exception(errorMessage);			  
		}
			
		val logicalTable = R2RMLLogicalTable.parse(logicalTableStatement.getObject.asResource)
//				try {
//					val conn = pOwner.getConn();
//					if(conn != null) {
//						logger.info("Building metadata for triples map: " + triplesMapName);
//						logicalTableAux.buildMetaData(conn);
//						logger.info("metadata built.");						
//					}
//					logicalTableAux
//				} catch{
//				  case e:Exception => {
//				    logger.error(e.getMessage());
//				    logicalTableAux
//				  }
//				}

		//rr:subjectMap
		val subjectMaps = R2RMLSubjectMap.extractSubjectMaps(tmResource);
		if(subjectMaps == null) {
			val errorMessage = "Missing rr:subjectMap";
			logger.error(errorMessage);
			throw new Exception(errorMessage);
		}
		if(subjectMaps.size > 1) {
			val errorMessage = "Multiple rr:subjectMap predicates are not allowed";
			logger.error(errorMessage);
			throw new Exception(errorMessage);
		}
		val subjectMap = subjectMaps.head
		
		//rr:predicateObjectMap SET
		val predicateObjectMaps = tmResource.listProperties(
		    Constants.R2RML_PREDICATEOBJECTMAP_PROPERTY).map(statement =>
				R2RMLPredicateObjectMap(statement.getObject.asResource)).toSet
		
		val tm = new R2RMLTriplesMap(logicalTable, subjectMap, predicateObjectMaps)
		tm.resource = tmResource
		tm.name = tmResource.getLocalName();
		tm;
	}
	
	
}

	