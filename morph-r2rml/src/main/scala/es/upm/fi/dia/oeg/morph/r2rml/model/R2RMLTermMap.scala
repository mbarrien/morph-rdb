package es.upm.fi.dia.oeg.morph.r2rml.model

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.RegexUtility
import scala.collection.JavaConversions._
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import java.sql.ResultSet
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import Zql.ZConstant
import java.util.HashMap
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import com.hp.hpl.jena.rdf.model.RDFNode
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElement
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor

abstract class R2RMLTermMap(val termMapType:Constants.MorphTermMapType.Value
    , termType:Option[String], val datatype:Option[String], val languageTag:Option[String])
    //extends R2RMLElement with IConstantTermMap with  IColumnTermMap with ITemplateTermMap 
extends MorphR2RMLElement with IConstantTermMap with IColumnTermMap with ITemplateTermMap
{
//	def this(termMapType:Constants.MorphTermMapType.Value, termType:Option[String]) = { 
//	  this(termMapType, termType, None, None)
//	}
	
  def this(rdfNode:RDFNode) = {
	  this(R2RMLTermMap.extractTermMapType(rdfNode), R2RMLTermMap.extractTermType(rdfNode)
	      , R2RMLTermMap.extractDatatype(rdfNode), R2RMLTermMap.extractLanguageTag(rdfNode));
	  this.rdfNode = rdfNode;
	  this.parse(rdfNode);
	}
	
	val logger = Logger.getLogger(this.getClass().getName());
	var rdfNode:RDFNode = null;
	
	def accept(visitor:MorphR2RMLElementVisitor) : Object = visitor.visit(this)
	
//	def parse(rdfNode:RDFNode) = {
//	  this.rdfNode = rdfNode;
//	  
//	  //if(rdfNode.isAnon()) {
//		  val resourceNode = rdfNode.asResource();
//			val constantStatement = resourceNode.getProperty(Constants.R2RML_CONSTANT_PROPERTY);
//			if(constantStatement != null) {
//				this.constantValue = constantStatement.getObject().toString();
//			} else {
//				val columnStatement = resourceNode.getProperty(Constants.R2RML_COLUMN_PROPERTY);
//				if(columnStatement != null) {
//					this.columnName = columnStatement.getObject().toString();
//				} else {
//					val templateStatement = resourceNode.getProperty(Constants.R2RML_TEMPLATE_PROPERTY);
//					if(templateStatement != null) {
//						this.templateString = templateStatement.getObject().toString();
//					} else {
//						val termMapType = this match {
//						  case _:R2RMLSubjectMap => { "SubjectMap"; } 
//						  case _:R2RMLPredicateMap => { "PredicateMap"; } 
//						  case _:R2RMLObjectMap => { "ObjectMap"; } 
//						  case _:R2RMLGraphMap => { "GraphMap"; } 
//						  case _ => { "TermMap"; }					  
//						}
//	
//						val errorMessage = "Invalid mapping for " + resourceNode.getLocalName();
//						logger.error(errorMessage);
//						throw new Exception(errorMessage);
//					}
//				}
//			}	    
//	  //} else { this.constantValue = rdfNode.toString(); }
//	}

	/**
	 * Contributor: Franck Michel
 * Decide the type of the term map (constant, column, reference, template) based on its properties,
 * and assign the value of the appropriate trait member: IConstantTermMap.constantValue, IColumnTermMap.columnName etc.
 *
 * If the term map resource has no property (constant, column, reference, template) then it means that this is a
 * constant term map like "[] rr:predicate ex:name".
 *
 * If the node passed in not a resource but a literal, then it means that we have a constant property wich object
 * is a literal and not an IRI or blank node, like in: "[] rr:object 'any string';"
 *
 * @param rdfNode the term map Jena resource
 */
	def parse(rdfNode: RDFNode): Unit = {
		this.rdfNode = rdfNode;

		if (!rdfNode.isLiteral) {
			val resourceNode = rdfNode.asResource();

			// Map from constant to the setter to use
			val termMapTypes = List(Constants.R2RML_CONSTANT_PROPERTY -> this.constantValue_= _, 
									Constants.R2RML_COLUMN_PROPERTY -> this.columnName_= _,
									Constants.R2RML_TEMPLATE_PROPERTY -> this.templateString_= _)
			// Find first non empty property, then use the associated setter to set the field.
			for (termMapType <- termMapTypes) termMapType match {
				case (propertyType, setter) => {
					val statement = resourceNode.getProperty(propertyType)
					if (statement != null) {
						setter(statement.getObject.toString)
						return
					}
				}
			}
			// If exiting loop, we are in the case of a constant property, like "[] rr:predicate ex:name"
		}
		// if !rdfNode.isLiteral, we are in the case of a constant property with a literal object, like "[] rr:object 'NAME'",
		this.constantValue = rdfNode.toString
	}

	def inferTermType() : String = this.termType.getOrElse(this.getDefaultTermType)

	def getDefaultTermType() : String = Constants.R2RML_IRI_URI

	def getReferencedColumns() : List[String] = this.termMapType match {
		case Constants.MorphTermMapType.ColumnTermMap => List(this.columnName)
		case Constants.MorphTermMapType.TemplateTermMap =>
			RegexUtility.getTemplateColumns(this.getOriginalValue, true).toList
		case _ => Nil
	}


	def getOriginalValue() : String = this.termMapType match {
	    case Constants.MorphTermMapType.ConstantTermMap => this.constantValue
	    case Constants.MorphTermMapType.ColumnTermMap => this.columnName
	    case Constants.MorphTermMapType.TemplateTermMap => this.templateString
	    case _ => null
	}

	def isBlankNode() : Boolean = Constants.R2RML_BLANKNODE_URI.equals(this.termType)

	override def toString() : String = {
		var result = this.termMapType match {
		  case Constants.MorphTermMapType.ConstantTermMap => { "rr:constant"; } 
		  case Constants.MorphTermMapType.ColumnTermMap => { "rr:column"; } 
		  case Constants.MorphTermMapType.TemplateTermMap =>  { "rr:template"; }
		  case _ => "";
		}	

		result += "::" + this.getOriginalValue();

//		if(this.termMapType == Constants.MorphTermMapType.ColumnTermMap) {
//			if(this.columnTypeName != null) {
//				result += ":" + this.columnTypeName;	
//			}
//		}

		return result;
	}

}

object R2RMLTermMap {
	val logger = Logger.getLogger(this.getClass().getName());
	
	def determineTermMapType(resource:Resource) = {
	  
	}
	
	def extractTermType(rdfNode:RDFNode) = rdfNode match {
	    case resource:Resource =>
			Option(resource.getProperty(Constants.R2RML_TERMTYPE_PROPERTY)).map(_.getObject.toString)
	    case _ => None
	}
	
	def extractTermMapType(rdfNode:RDFNode) = rdfNode match {
	    case resource:Resource => {
			// TODO: Refactor copy and paste from morph-base R2RMLMappingDocument
			val termMapTypes = List(Constants.R2RML_CONSTANT_PROPERTY -> Constants.MorphTermMapType.ConstantTermMap,
									Constants.R2RML_COLUMN_PROPERTY -> Constants.MorphTermMapType.ColumnTermMap,
									Constants.R2RML_TEMPLATE_PROPERTY -> Constants.MorphTermMapType.TemplateTermMap)
			termMapTypes.find({
				case (propertyType, result) => resource.getProperty(propertyType) != null
			}).map(_._2).getOrElse({
				val errorMessage = "Invalid mapping for " + resource.getLocalName()
				logger.error(errorMessage)
				throw new Exception(errorMessage)
			})
	    }
	    case _ => Constants.MorphTermMapType.ConstantTermMap
	}
	
	
	def extractDatatype(rdfNode:RDFNode) = rdfNode match {
	    case resource:Resource =>
			Option(resource.getProperty(Constants.R2RML_DATATYPE_PROPERTY)).map(_.getObject.toString)
	    case _ => None
	}
	
	def extractLanguageTag(rdfNode:RDFNode) = rdfNode match {
		case resource:Resource =>
			Option(resource.getProperty(Constants.R2RML_LANGUAGE_PROPERTY)).map(_.getObject.toString)
		case _ => None
	}
	
	def extractCoreProperties(rdfNode:RDFNode) = {
	  val termMapType=R2RMLTermMap.extractTermMapType(rdfNode);
	  val datatype=R2RMLTermMap.extractDatatype(rdfNode);
	  val languageTag=R2RMLTermMap.extractLanguageTag(rdfNode);
	  val termType=R2RMLTermMap.extractTermType(rdfNode);
	  
	  val coreProperties = (termMapType, termType, datatype, languageTag) 
	  coreProperties;
	}
	
	def extractTermMaps(resource:Resource, termMapType:Constants.MorphPOS.Value) 
	: Set[R2RMLTermMap]= {
		val mapProperty1 = termMapType match {
			case Constants.MorphPOS.sub => Constants.R2RML_SUBJECTMAP_PROPERTY
			case Constants.MorphPOS.pre => Constants.R2RML_PREDICATEMAP_PROPERTY
			case Constants.MorphPOS.obj => Constants.R2RML_OBJECTMAP_PROPERTY
			case Constants.MorphPOS.graph => Constants.R2RML_GRAPHMAP_PROPERTY
		}
		  
		val maps1 = resource.listProperties(mapProperty1).toList().flatMap(mapStatement => {
			val mapStatementObject = mapStatement.getObject
			termMapType match {
				case Constants.MorphPOS.sub => Some(R2RMLSubjectMap(mapStatementObject))
				case Constants.MorphPOS.pre => Some(R2RMLPredicateMap(mapStatementObject))
				case Constants.MorphPOS.obj => {
					val mapStatementObjectResource = mapStatementObject.asResource
					if(!R2RMLRefObjectMap.isRefObjectMap(mapStatementObjectResource)) {
						Some(R2RMLObjectMap(mapStatementObject))
					} else None
				}
				case Constants.MorphPOS.graph => {
					val gm = R2RMLGraphMap(mapStatementObject)
					if(Constants.R2RML_DEFAULT_GRAPH_URI.equals(gm.getOriginalValue)) {
						None
					} else Some(gm)
				}
			}
		})
		
		val mapProperty2 = termMapType match {
			case Constants.MorphPOS.sub => Constants.R2RML_SUBJECT_PROPERTY
			case Constants.MorphPOS.pre => Constants.R2RML_PREDICATE_PROPERTY
			case Constants.MorphPOS.obj => Constants.R2RML_OBJECT_PROPERTY
			case Constants.MorphPOS.graph => Constants.R2RML_GRAPH_PROPERTY
		}
		val maps2 = resource.listProperties(mapProperty2).toList().flatMap(mapStatement => {
			val mapStatementObject = mapStatement.getObject
			termMapType match {
				case Constants.MorphPOS.sub => {
					val sm = new R2RMLSubjectMap(Constants.MorphTermMapType.ConstantTermMap
					    , Some(Constants.R2RML_IRI_URI), None, None, Set.empty, Set.empty)
					sm.parse(mapStatementObject)
					Some(sm)
				}
				case Constants.MorphPOS.pre => {
					val pm = new R2RMLPredicateMap(Constants.MorphTermMapType.ConstantTermMap
					    , Some(Constants.R2RML_IRI_URI), None, None)
					pm.parse(mapStatementObject)
					Some(pm)
				}
				case Constants.MorphPOS.obj => {
					val om = new R2RMLObjectMap(Constants.MorphTermMapType.ConstantTermMap
					    , Some(Constants.R2RML_IRI_URI), None, None)
					om.parse(mapStatementObject)
					Some(om)
				}
				case Constants.MorphPOS.graph => {
					val gm = new R2RMLGraphMap(Constants.MorphTermMapType.ConstantTermMap
					    , Some(Constants.R2RML_IRI_URI), None, None)
					gm.parse(mapStatementObject)
					if(Constants.R2RML_DEFAULT_GRAPH_URI.equals(gm.getOriginalValue)) {
						None
					} else Some(gm)
				}
			}
		})
		(maps1 ++ maps2).toSet
	}
}
