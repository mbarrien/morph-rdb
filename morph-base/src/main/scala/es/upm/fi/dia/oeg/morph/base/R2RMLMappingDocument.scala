package es.upm.fi.dia.oeg.morph.base

import scala.collection.JavaConversions._
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.rdf.model.Resource
import org.apache.log4j.Logger

class R2RMLMappingDocument(mappingFile : String) {
	val logger = Logger.getLogger("R2RMLMappingDocument");

	val XSDIntegerURI = XSDDatatype.XSDinteger.getURI();
	val XSDDoubleURI = XSDDatatype.XSDdouble.getURI();
	val XSDDateTimeURI = XSDDatatype.XSDdateTime.getURI();
	// val datatypesMap:Map[String, String] = Map("INT" -> XSDIntegerURI,
	// 										   "DOUBLE" -> XSDDoubleURI);
	// 										   "DATETIME" -> XSDDateTimeURI)
	
	val model = ModelFactory.createDefaultModel();
	val in = FileManager.get().open( mappingFile );
	model.read(in, null, "TURTLE");

	def getTriplesMapResourcesByRRClass(classURI : String) =
		model.listSubjectsWithProperty(Constants.R2RML_SUBJECTMAP_PROPERTY).toList().filter(triplesMapResource => {
			val subjectMapResource = triplesMapResource.getPropertyResourceValue(Constants.R2RML_SUBJECTMAP_PROPERTY)
			subjectMapResource != null && {
				val rrClassResource = subjectMapResource.getPropertyResourceValue(Constants.R2RML_CLASS_PROPERTY)
				rrClassResource != null && rrClassResource.getURI.equals(classURI)
			}
		})

	def getPredicateObjectMapResources(triplesMapResources : List[Resource]) : List[Resource] =
		triplesMapResources.flatMap(this.getPredicateObjectMapResources(_))

		
	def getPredicateObjectMapResources(triplesMapResource : Resource) : List[Resource] =
		triplesMapResource.listProperties(Constants.R2RML_PREDICATEOBJECTMAP_PROPERTY).map(
			_.getObject.asResource).toList
	
	def getRRLogicalTable(triplesMapResource : Resource) =
		triplesMapResource.getPropertyResourceValue(Constants.R2RML_LOGICALTABLE_PROPERTY)

	def getRRSubjectMapResource(triplesMapResource : Resource) =
		triplesMapResource.getPropertyResourceValue(Constants.R2RML_SUBJECTMAP_PROPERTY)

	def getRRLogicalTableTableName(triplesMapResource : Resource) = {
		val rrLogicalTableResource = this.getRRLogicalTable(triplesMapResource);
		val rrTableNameResource = rrLogicalTableResource.getPropertyResourceValue(Constants.R2RML_TABLENAME_PROPERTY);
		if( rrTableNameResource != null) rrTableNameResource.asLiteral.getValue.toString else null
	}
	
	def getObjectMapResource(predicateObjectMapResource : Resource) = {
	  val objectMapResource = predicateObjectMapResource.getPropertyResourceValue(Constants.R2RML_OBJECTMAP_PROPERTY);
	  val parentTriplesMap = objectMapResource.getPropertyResourceValue(Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
	  if(parentTriplesMap == null) objectMapResource else null
	}

	def getRefObjectMapResource(predicateObjectMapResource : Resource) = {
	  val objectMapResource = predicateObjectMapResource.getPropertyResourceValue(Constants.R2RML_OBJECTMAP_PROPERTY);
	  val parentTriplesMap = objectMapResource.getPropertyResourceValue(Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
	  if(parentTriplesMap != null) objectMapResource else null
	}
		
	def getParentTriplesMapResource(objectMapResource : Resource) =
		objectMapResource.getPropertyResourceValue(Constants.R2RML_PARENTTRIPLESMAP_PROPERTY)
	
	def getParentTriplesMapLogicalTableResource(objectMapResource : Resource) =
		// this.getParentTriplesMapResource(objectMapResource)
		this.getRRLogicalTable(Constants.R2RML_LOGICALTABLE_PROPERTY)

	def getTermTypeResource(termMapResource : Resource) =
		termMapResource.getPropertyResourceValue(Constants.R2RML_TERMTYPE_PROPERTY)

	def getRRColumnResource(termMapResource : Resource) =
		termMapResource.getProperty(Constants.R2RML_COLUMN_PROPERTY)

	def getRRTemplateResource(termMapResource : Resource) =
		termMapResource.getProperty(Constants.R2RML_TEMPLATE_PROPERTY).getObject()

	def getDatatypeResource(termMapResource : Resource) =
		termMapResource.getPropertyResourceValue(Constants.R2RML_DATATYPE_PROPERTY)
	
	
	def getTermMapType(termMapResource : Resource) = {
		if(termMapResource == null) {
			logger.debug("termMapResource is null");
		}
		val termMapTypes = List(Constants.R2RML_CONSTANT_PROPERTY -> Constants.MorphTermMapType.ConstantTermMap, 
								Constants.R2RML_COLUMN_PROPERTY -> Constants.MorphTermMapType.ColumnTermMap,
								Constants.R2RML_TEMPLATE_PROPERTY -> Constants.MorphTermMapType.TemplateTermMap)
		termMapTypes.find({
			case (propertyType, result) => termMapResource.getProperty(propertyType) != null
		}).map(_._2).getOrElse(Constants.MorphTermMapType.InvalidTermMapType)
	}
	
	def getTemplateValues(termMapResource : Resource, uri : String ) : Map[String, String] =
		if(this.getTermMapType(termMapResource) == Constants.MorphTermMapType.TemplateTermMap) {
			val templateString = this.getRRTemplateResource(termMapResource).asLiteral().getValue().toString()
			RegexUtility.getTemplateMatching(templateString, uri).toMap
		} else {
			Map.empty
		}
}
