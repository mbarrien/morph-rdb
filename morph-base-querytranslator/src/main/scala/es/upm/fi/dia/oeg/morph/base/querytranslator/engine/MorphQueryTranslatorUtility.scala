package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.hp.hpl.jena.graph.{Triple, Node}
import com.hp.hpl.jena.sparql.algebra.Op
import com.hp.hpl.jena.sparql.algebra.op.{OpLeftJoin, OpJoin, OpFilter, OpUnion, OpBGP}
import Zql.ZSelectItem
import es.upm.fi.dia.oeg.morph.base.sql.{MorphSQLSelectItem, MorphSQLUtility}
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping


class MorphQueryTranslatorUtility {

}

object MorphQueryTranslatorUtility {

	/** Take the union of keys, but if keys overlap, take the intersection of the sets. */
	def mapsIntersection(map1:Map[Node, Set[MorphBaseClassMapping]] , map2:Map[Node, Set[MorphBaseClassMapping]]) 
	: Map[Node, Set[MorphBaseClassMapping]] =
		// Convert to sequences to preserve duplicates and merge, then regroup by key,
		// then transform resulting list of sets by unioning all the sets in the list together
		// Modified from http://stackoverflow.com/questions/20047080/scala-merge-map
		(map1.toSeq ++ map2.toSeq).groupBy(_._1).mapValues(_.map(_._2).reduce(_.union(_)))

	def mapsIntersection(maps:List[Map[Node, Set[MorphBaseClassMapping]]]) 
	: Map[Node, Set[MorphBaseClassMapping]] = {
		if(maps == null || maps.isEmpty) {
			Map.empty;
		} else {
			maps.reduceLeft(this.mapsIntersection(_, _))
		}
	}

	def isTriplePattern(opBGP:OpBGP ) : Boolean =
		opBGP.getPattern().size() == 1
	
	def isSTG(triples:List[Triple]) : Boolean =
		triples.size() > 1 && triples.forall(_.getSubject() == triples(0).getSubject())
	
	def isSTG(opBGP : OpBGP) : Boolean =
		this.isSTG(opBGP.getPattern().getList().toList)
	
	def getFirstTBEndIndex(triples:java.util.List[Triple] ) =
		triples.prefixLength(_.getSubject() == triples(0).getSubject())
	
	def terms(op:Op) : java.util.Collection[Node] =
		this.getTerms(op).asJavaCollection

	def getTerms(op:Op) : Set[Node] = {
		val result : Set[Node] = {
			op match {
				case bgp:OpBGP => {
					val triples = bgp.getPattern().getList();
					triples.flatMap(
						triple => List(triple.getSubject(), triple.getPredicate(), triple.getObject())
					).toSet.filter(!_.isConcrete())
				}
				case leftJoin:OpLeftJoin  => {
					val resultLeft = this.getTerms(leftJoin.getLeft());
					val resultRight = this.getTerms(leftJoin.getRight());
					resultLeft ++ resultRight 
				} 
				case opJoin:OpJoin => {
					val resultLeft = this.getTerms(opJoin.getLeft());
					val resultRight = this.getTerms(opJoin.getRight());
					resultLeft ++ resultRight
				} 
				case filter:OpFilter => {
					this.getTerms(filter.getSubOp());
				} 
				case opUnion:OpUnion => {
					val resultLeft = this.getTerms(opUnion.getLeft());
					val resultRight = this.getTerms(opUnion.getRight());
					resultLeft ++ resultRight
				}
				case _ => Set.empty;
			}
		}

		result;
	}

	def generateMappingIdSelectItems(nodes:List[Node], selectItems:List[ZSelectItem]
	, pPrefix:String , dbType:String) : List[ZSelectItem] = {
		val prefix = pPrefix match {
			case x if x == null => ""
			case x if !x.endsWith(".") => pPrefix + "."
			case _ => pPrefix
		}

		nodes.flatMap(term => {
			if(term.isVariable()) {
				val mappingsSelectItemsAux = MorphSQLUtility.getSelectItemsMapPrefix(
				    selectItems, term, prefix, dbType);
				mappingsSelectItemsAux.map(mappingsSelectItemAux => {
					val mappingSelectItemAuxAlias = mappingsSelectItemAux.getAlias();
					val newSelectItem = MorphSQLSelectItem.apply(
							mappingSelectItemAuxAlias, prefix, dbType, null);
					newSelectItem;				  
				})
			} else {
				Nil
			}
		})
	}
}
