package es.upm.fi.dia.oeg.morph.base.querytranslator
import scala.collection.JavaConversions._
import Zql.ZSelectItem
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import java.util.Collection
import org.apache.log4j.Logger
import scala.collection.mutable.LinkedHashSet
import es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslator
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping

class MorphBasePRSQLGenerator(
    val owner:IQueryTranslator 
    ) {

  val logger = Logger.getLogger("MorphBasePRSQLGenerator");
	val databaseType = {
		if(this.owner == null) {null}
		else {this.owner.getDatabaseType();}
	}
	
	def  genPRSQL(tp:Triple , alphaResult:MorphAlphaResult , betaGenerator:MorphBaseBetaGenerator
	    , nameGenerator:NameGenerator, cmSubject:AbstractConceptMapping, predicateURI:String , unboundedPredicate:Boolean) 
	: Collection[ZSelectItem] = {
		val tpSubject = tp.getSubject();
		val tpPredicate = tp.getPredicate();
		val tpObject = tp.getObject();
		

		var prList : List[ZSelectItem ]= Nil;

		val selectItemsSubjects = this.genPRSQLSubject(tp, alphaResult, betaGenerator, nameGenerator, cmSubject);
		prList = prList ::: selectItemsSubjects.toList;

		if(tpPredicate != tpSubject) {
			//line 22
			if(unboundedPredicate) {
				val selectItemPredicate = this.genPRSQLPredicate(tp, alphaResult, betaGenerator, nameGenerator, predicateURI);
				if(selectItemPredicate != null) {
					prList = prList ::: List(selectItemPredicate);
				}				
			}
		}

		if(tpObject != tpSubject && tpObject != tpPredicate) {
			val columnType = {
				if(tpPredicate.isVariable()) {
					if(Constants.DATABASE_POSTGRESQL.equalsIgnoreCase(databaseType)) {
						Constants.POSTGRESQL_COLUMN_TYPE_TEXT;	
					} else if(Constants.DATABASE_MONETDB.equalsIgnoreCase(databaseType)) {
						Constants.MONETDB_COLUMN_TYPE_TEXT;
					} else {
						Constants.MONETDB_COLUMN_TYPE_TEXT;
					}
				} else {
				  null
				}
			}
			
			//line 23
			val objectSelectItems = this.genPRSQLObject(tp, alphaResult, betaGenerator, nameGenerator
			    ,cmSubject, predicateURI, columnType);
			prList = prList ::: objectSelectItems.toList;
		}

		logger.debug("genPRSQL = " + prList);
		prList;
	}
	
	def genPRSQLObject(tp:Triple, alphaResult:MorphAlphaResult, betaGenerator:MorphBaseBetaGenerator 
	    , nameGenerator:NameGenerator , cmSubject:AbstractConceptMapping , predicateURI:String , columnType:String) 
	: Collection[ZSelectItem] = {
		
		def betaObjSelectItems = betaGenerator.calculateBetaObject(tp, cmSubject, predicateURI, alphaResult);
		val selectItems = for(i <- 0 until betaObjSelectItems.size()) yield {
			val betaObjSelectItem = betaObjSelectItems.get(i);
			val selectItem = MorphSQLSelectItem.apply(betaObjSelectItem, databaseType, columnType);
			
			val selectItemAliasAux = nameGenerator.generateName(tp.getObject());
			val selectItemAlias = {
				if(selectItemAliasAux != null) {
					if(betaObjSelectItems.size() > 1) {
						selectItemAliasAux + "_" + i;
					} else {
					  selectItemAliasAux
					}
				} else {
				  selectItemAliasAux
				}
			}
			
			if(selectItemAlias != null) {
			  selectItem.setAlias(selectItemAlias);
			}
			
			selectItem; //line 23
		}
		selectItems;
	}

	def  genPRSQLPredicate(tp:Triple, alphaResult:MorphAlphaResult , betaGenerator:MorphBaseBetaGenerator 
	    , nameGenerator:NameGenerator , predicateURI:String ) : ZSelectItem = {
			val betaPre = betaGenerator.calculateBetaPredicate(predicateURI);
			val selectItem = MorphSQLSelectItem.apply(betaPre, this.databaseType, "text");

			val alias = nameGenerator.generateName(tp.getPredicate());
			selectItem.setAlias(alias);
			return selectItem;

	}
	
	def  genPRSQLSubject(tp:Triple , alphaResult:MorphAlphaResult , betaGenerator:MorphBaseBetaGenerator 
	    , nameGenerator:NameGenerator , cmSubject:AbstractConceptMapping ) : Collection[ZSelectItem] = {
		val tpSubject = tp.getSubject();
		
		val prSubjects = {
			if(!tpSubject.isBlank()) {
				val betaSubSelectItems = betaGenerator.calculateBetaSubject(tp, cmSubject, alphaResult);
				for(i <- 0 until betaSubSelectItems.size()) yield {
					val betaSub = betaSubSelectItems.get(i);
						
					val selectItem = MorphSQLSelectItem.apply(betaSub, databaseType);
					val selectItemSubjectAliasAux = nameGenerator.generateName(tpSubject);
					val selectItemSubjectAlias = {
						if(betaSubSelectItems.size() > 1) {
							selectItemSubjectAliasAux + "_" + i;
						} else {
							 selectItemSubjectAliasAux
						}
					}
						
					selectItem.setAlias(selectItemSubjectAlias);
					selectItem;
				}
			} else {
			  Nil
			}		  
		}
		prSubjects
	}
	


	def  genPRSQLSTG(stg:Collection[Triple],alphaResult:MorphAlphaResult , betaGenerator:MorphBaseBetaGenerator 
	    ,nameGenerator:NameGenerator , cmSubject:AbstractConceptMapping ) : Collection[ZSelectItem] = {
		
		val firstTriple = stg.iterator.next();
		val selectItemsSubjects = this.genPRSQLSubject(firstTriple, alphaResult, betaGenerator, nameGenerator, cmSubject);

		val tpSubject = firstTriple.getSubject();
		var selectItemsSTGObjects : LinkedHashSet[ZSelectItem] = new LinkedHashSet[ZSelectItem]();
		
		for(tp <- stg)  {
			val tpPredicate = tp.getPredicate();
			if(!tpPredicate.isURI()) {
				val errorMessage = "Only bounded predicate is supported in STG.";
				logger.warn(errorMessage);
			}
			val predicateURI = tpPredicate.getURI();
			
			if(RDF.`type`.getURI().equals(predicateURI)) {
				//do nothing
			} else {
				val tpObject = tp.getObject();
				if(tpPredicate != tpSubject) {
					val selectItemPredicate = this.genPRSQLPredicate(tp, alphaResult
					    , betaGenerator, nameGenerator,predicateURI);
					if(selectItemPredicate != null) {
						//prList.add(selectItemPredicate);	
					}
					
				}
				if(tpObject != tpSubject && tpObject != tpPredicate) {
					val selectItemsObject = this.genPRSQLObject(tp, alphaResult, betaGenerator
					    , nameGenerator, cmSubject, predicateURI, null);
					selectItemsSTGObjects  = selectItemsSTGObjects ++ selectItemsObject;
				} else {
				}				
			}
		}

		val prList = selectItemsSubjects.toList ::: selectItemsSTGObjects.toList;
		prList;
	}

}