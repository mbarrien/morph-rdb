package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.Constants
import java.util.Collection
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElement
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import com.hp.hpl.jena.rdf.model.Model

class R2RMLMappingDocument(classMappings:Iterable[R2RMLTriplesMap]) 
extends MorphBaseMappingDocument(classMappings) with MorphR2RMLElement {
	override val logger = Logger.getLogger(this.getClass());
   
	def buildMetaData(conn:Connection, databaseName:String
       , databaseType:String) = {
		if(conn != null && this.dbMetaData == None) {
			val newMetaData = MorphDatabaseMetaData(conn, databaseName, databaseType);
			this.dbMetaData = Some(newMetaData);
			this.classMappings.foreach(cm => cm.buildMetaData(this.dbMetaData));       
		}
	}

	def accept(visitor:MorphR2RMLElementVisitor) : Object = visitor.visit(this)
	
	override def getMappedProperties() : Iterable[String] =
		this.classMappings.flatMap(
			_.asInstanceOf[R2RMLTriplesMap].getPropertyMappings.flatMap(_.getMappedPredicateNames)
		)

//	def setTriplesMaps(triplesMaps:Collection[MorphBaseClassMapping] ) = {
//		this.classMappings = triplesMaps.toSet;
//	}	

	def getParentTriplesMap(refObjectMap:R2RMLRefObjectMap) : R2RMLTriplesMap = {
	  val parentTripleMapResources = this.classMappings.flatMap(
	    _.asInstanceOf[R2RMLTriplesMap].predicateObjectMaps.flatMap(
			_.refObjectMaps.filter(_ == refObjectMap).map(_.parentTriplesMapResource)
	    )
	  )
	  
	  val parentTripleMaps = this.classMappings.filter(cm => {
		val tm = cm.asInstanceOf[R2RMLTriplesMap];
	    parentTripleMapResources.exists(_ == tm.resource)
	  })
	  //contribution from Frank Michel
	  if (parentTripleMaps.isEmpty) {
		  throw new Exception("Error: referenced parent triples map does not exist: " + parentTripleMapResources)
	  }
	  parentTripleMaps.head.asInstanceOf[R2RMLTriplesMap]
	}
	
	override def getPossibleRange(predicateURI:String ) : Iterable[MorphBaseClassMapping] =
		this.getPropertyMappingsByPropertyURI(predicateURI).flatMap(this.getPossibleRange(_)).toSet

	override def getPossibleRange(predicateURI:String , cm:MorphBaseClassMapping ) 
	: Iterable[MorphBaseClassMapping] =
		cm.getPropertyMappings(predicateURI).flatMap(this.getPossibleRange(_)).toSet

	override def  getPossibleRange(pm:MorphBasePropertyMapping) 
	: Iterable[MorphBaseClassMapping] = {
		
		val pom = pm.asInstanceOf[R2RMLPredicateObjectMap];
		val om = pom.getObjectMap(0);
		val rom = pom.getRefObjectMap(0);
		
		val result:Iterable[MorphBaseClassMapping] = if(om != null && rom == null) {
			val inferredTermType = om.inferTermType;
			if (Constants.R2RML_IRI_URI.equals(inferredTermType) &&
				Constants.MorphTermMapType.TemplateTermMap == om.termMapType) {
				this.getClassMappingsByInstanceURI(om.getTemplateString)
			} else Nil
		} else if(rom != null && om == null) {
			//val parentTriplesMap = rom.getParentTriplesMap().asInstanceOf[R2RMLTriplesMap];
			val parentTriplesMap = this.getParentTriplesMap(rom);
			
			val parentSubjectMap = parentTriplesMap.subjectMap;
			if(parentSubjectMap.termMapType == Constants.MorphTermMapType.TemplateTermMap) {
				this.getClassMappingsByInstanceURI(parentSubjectMap.getTemplateString).filter(
					_.asInstanceOf[R2RMLTriplesMap].subjectMap.classURIs.nonEmpty)
			} else {
				List(parentTriplesMap);	
			}
		} else {
		  Nil
		}
		result.toSet
	}
	
	def getClassMappingsByInstanceTemplate(templateValue:String) 
	: Iterable[MorphBaseClassMapping] =
		this.classMappings.filter(
			_.asInstanceOf[R2RMLTriplesMap].subjectMap.templateString.startsWith(templateValue))

	def getClassMappingsByInstanceURI(instanceURI:String):Iterable[MorphBaseClassMapping] =
		this.classMappings.filter(_.isPossibleInstance(instanceURI))
}

object R2RMLMappingDocument {
	val logger = Logger.getLogger(this.getClass().getName());
	
	def apply(mdPath:String)
	: R2RMLMappingDocument = {
	  R2RMLMappingDocument(mdPath, null, null);
	}
	
	def apply(mdPath:String, props:MorphProperties
	    , connection:Connection)
	: R2RMLMappingDocument = {

		val model = ModelFactory.createDefaultModel();
		// use the FileManager to find the input file
		val in = FileManager.get().open( mdPath );
		if (in == null) {
			throw new IllegalArgumentException(
					"Mapping File: " + mdPath + " not found");
		}

		logger.info("Parsing mapping document " + mdPath);
		// read the Turtle file
		model.read(in, null, "TURTLE");
		
		//contribution from Frank Michel, inferring triples map
		inferTriplesMaps(model);
		 
		val triplesMapResources = model.listResourcesWithProperty(RDF.`type`
				, Constants.R2RML_TRIPLESMAP_CLASS);
		val classMappings = if(triplesMapResources != null) {
		  triplesMapResources.map(triplesMapResource => {
				val triplesMapKey = triplesMapResource.getLocalName();
				val tm = R2RMLTriplesMap(triplesMapResource);
				tm.id = triplesMapKey;
				tm;		    
		  })
		} else {
		  Set.empty
		}
		
		val md = new R2RMLMappingDocument(classMappings.toSet);
		md.mappingDocumentPath = mdPath;
	
		if(connection != null) {
		  //BUILDING METADATA
		   try {
			   md.buildMetaData(connection, props.databaseName, props.databaseType );
		   } catch {
		     case e:Exception => { logger.warn("Error while building metadata.") }
		   }
		}
   
		//md.configurationProperties = props;
		md.mappingDocumentPrefixMap = model.getNsPrefixMap().toMap;
		md
		
	}
	
	/**
	 *  Add triples with rdf:type rr:TriplesMap for resources that have one rr:logicalTable
	 *  Contribution from Frank Michel
	 */
	private def inferTriplesMaps(model: Model) {
		val stmtsLogTab = model.listStatements(null, Constants.R2RML_LOGICALTABLE_PROPERTY, null);
		for (stmt <- stmtsLogTab) {
			val stmtType = model.createStatement(stmt.getSubject, RDF.`type`, Constants.R2RML_TRIPLESMAP_CLASS);
			model.add(stmtType)
		}
	}

}