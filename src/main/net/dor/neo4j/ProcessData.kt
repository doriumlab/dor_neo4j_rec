package dor.neo4j

import org.bouncycastle.asn1.DEREncodableVector
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.index.Index
import org.neo4j.graphdb.index.IndexManager
import org.neo4j.logging.Log
import org.neo4j.graphdb.*

import org.neo4j.helpers.collection.MapUtil.stringMap
import org.neo4j.graphdb.QueryExecutionType.query
import org.neo4j.procedure.*
import java.time.Year
import java.util.stream.Stream
import javax.xml.crypto.Data
import org.neo4j.cypher.internal.spi.v3_1.codegen.Methods.row
import org.neo4j.cypher.internal.`InternalExecutionResult$class`.columns
import org.neo4j.cypher.internal.`InternalExecutionResult$class`.columns
import org.neo4j.cypher.internal.compiler.v3_1.codegen.ir.expressions.TypeOf
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions.Str
import org.neo4j.graphdb.QueryExecutionType.query
import java.security.MessageDigest
import java.util.*
import javax.management.relation.Relation
import javax.swing.text.StyledEditorKit
import kotlin.collections.ArrayList
import kotlin.collections.HashMap


class ProcessData {


    enum class Relations {
        SIMPLECONTENT_IN_SITE,
        SIMPLECONTENT_IN_PAGE,
        SGC_IN_SITE,
        RS_HAS_SESSION,
        Seen,
        USER_IN_RS,
        RS_IN_SITECONFIG,
        PRODUCT_IN_RS,
        FACT_IN_SITE,
        FACTDETECT_IN_FACT,
        PRODUCTLOG_HAS_PRODUCT,
        PRODUCT_HAS_BRAND,
        USER_HAS_SESSION,
        USER_HAS_LIKE_PRODUCT,
        USER_HAS_LIKE_BRAND,
        USER_HAS_GBEENPROFILE,
        FACT_HAS_PRODUCT,
        LOG_IN_SITE,
        FACTDETECT_HAS_PRODUCT,
        HAS_CHILD,
        NEGATIVE_FACTDETECT_IN_FACT,
        AND_FACTDETECT_IN_FACT,
        RPC_IS_RPC_CHILD,
        RP_CATEGORY_IN_SITE,
        FACTDETECT_IN_SITE,
        RP_CATEGORY_HAS_FACTDETECT,
        NEXT,
        PRICE_HAS_PRODUCT,
        CATEGORYDETECT_IN_SITE,
        RP_CATEGORY_HAS_CATEGORYDETECT,
        FACT_HAS_CATEGORY,
        RP_CATEGORY_HAS_PRODUCT,
        CATEGORYDETECT_HAS_PRODUCT,
        NEGATIVE_CATEGORYDETECT_IN_RPCATEGORY,
        AND_CATEGORYDETECT_IN_RPCATEGORY,
        CATEGORYDETECT_IN_RPCATEGORY,
        NEWSLETTER_IN_SITE,
        HAS_DEFAULT_NEWSLETTER,
        SUBSCRIBER_IN_NEWSLETTER,
        HAS_FIRST_PAGE,
        HAS_DEFAULT_LAYOUT,
        HAS_DEFAULTADMIN_LAYOUT,
        PFV_IN_SITE,
        BRANDDETECT_IN_SITE,
        BRAND_IN_SITE,
        BRANDDEDETECT_IN_BRAND,
        AND_BRANDDEDETECT_IN_BRAND,
        NEGATIVE_BRANDDEDETECT_IN_BRAND,
        BRANDDETECT_HAS_PRODUCT,
        PRODUCT_HAS_DESCRIPTION,
        PRODUCT_HAS_PERSIANTITLE,
        PRODUCT_HAS_ENGLISHTITLE,
        PRODUCT_HAS_MAIN_BRAND,
        PRODUCT_HAS_MAIN_RPCATEGORY
    }

    @Context
    lateinit var db: GraphDatabaseService


    @Context
    lateinit var log: Log

    @UserFunction(name = "adt.hello")
    fun sayHello(@Name("hello") zz: String): String {
        return "$zz hello"
    }

    @UserFunction(name = "adt.convertToSHA256")
    fun covertToSHA256(@Name("Title") Title: String): String {
        val bytes = Title.toByteArray()
        val md = MessageDigest.getInstance("SHA-256")
        val digest = md.digest(bytes)
        return digest.fold("", { str, it -> str + "%02x".format(it) })
    }

    @UserFunction(name = "adt.generateUUID")
    fun generateUUID(): String{
        val uuid : String = UUID.randomUUID().toString()
        return  uuid
    }

    @UserFunction(name = "adt.counter")
    fun counter(@Name("Detect") pattern: String, @Name("Description") text: String): Number {
        var sTemp = text.toLowerCase()
        var counter = 0

        while (sTemp.length > 0) {
            val index = sTemp.indexOf(pattern)
            if (index == -1) break
            sTemp = sTemp.substring(index + pattern.length, sTemp.length)
            counter++
        }
        return counter
    }

    @Procedure(name = "adt.createProduct")
    fun createProduct(@Name("Product") product : Any) {

        product as Product

        val q = """CREATE (c:Product { product })
                   WITH c
                   MATCH (rs:RS)
                   WHERE rs.SiteId = "50cfc9e8-402b-495b-8ed4-66dcb2b3aadd"
                   CREATE Unique (rs)<-[:${Relations.PRODUCT_IN_RS.toString()}]-(c)
                   WITH c
                   WITH SPLIT(c.Description," ") as words, c
                   UNWIND range(0,size(words)-2) as idx
                   MERGE (w1:Word {Title:words[idx],HashTitle: adt.covertToSHA256(words[idx]), Id:adt.generateUUID()})
                   MERGE (w2:Word {Title:words[idx+1]})
                   MERGE (w1)-[:NEXT]->(w2)
                   """.trimMargin()

        db.execute(q)
    }

    @Procedure(name = "adt.defineProduct", mode = Mode.WRITE)
    fun defineProduct(@Name("HashTitle") hashTitle: String) {
        val product: Node = db.findNode(EngineLable.productLabel(), "HashTitle", hashTitle)
        var s = analyseProduct(product)
        log.info("status: " + s.toString())
    }

    @Procedure(name = "adt.defineAllProducts", mode = Mode.WRITE)
    fun defineAllProducts() {
        try {

            val startTime = System.currentTimeMillis()

            analyseAllProduct()

            val endTime = System.currentTimeMillis()
            val duration = (endTime - startTime)
            log.info(duration.toString())
        } catch (e: Exception) {
            log.info("${e.message}")
        }


    }

    private fun defineProductBrand(hashTitle: String): Node? {
        try {
            val product: Node = db.findNode(EngineLable.productLabel(), "HashTitle", hashTitle)
            val productContent: String = "${product.getProperty("Description")} ${product.getProperty("FaTitle")} ${product.getProperty("EnTitle")}".toLowerCase()

            val query: String = """MATCH (s:SiteConfiguration)-[:${Relations.BRAND_IN_SITE.toString()}]-(c:Brand)-[:${Relations.BRANDDEDETECT_IN_BRAND.toString()}]-(cd:BrandDetect)
                                   WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                                   WITH c,cd, '$productContent' as productDescription
                                   WHERE productDescription CONTAINS toString(' ' + cd.Title +' ')
                                   RETURN c as brand, collect(cd) as brandDetect""".trimMargin()
//                                   UNION
//                                   MATCH (s:SiteConfiguration)-[:${Relations.BRAND_IN_SITE.toString()}]-(c:Brand)-[:${Relations.AND_BRANDDEDETECT_IN_BRAND.toString()}]-(cd:BrandDetect)
//                                   WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
//                                   WITH c,cd, '$productContent' as productDescription,collect(cd.Title) as detectTitle, collect(cd.Id) as detectId
//                                   WHERE all (x IN detectTitle WHERE productDescription CONTAINS toString(' ' + x +' '))
//                                   WITH c,cd, detectTitle, detectId
//                                   MATCH (s:SiteConfiguration)-[:${Relations.BRANDDETECT_IN_SITE.toString()}]-(cd2:BrandDetect)
//                                   WHERE cd2.Id in detectId
//                                   RETURN c as brand, collect(cd2) as brandDetect

            val brandDetectCountList = mutableListOf<DetectCountViewModel>()

            val oldRelBrand: Iterable<Relationship> = product.getRelationships(RelationshipType { Relations.PRODUCT_HAS_BRAND.toString() }, Direction.OUTGOING)
            oldRelBrand.forEach {
                it.delete()
            }

            val oldRelBrandDetect: Iterable<Relationship> = product.getRelationships(RelationshipType { Relations.BRANDDETECT_HAS_PRODUCT.toString() }, Direction.OUTGOING)
            oldRelBrandDetect.forEach {
                it.delete()
            }

            val oldRel: Iterable<Relationship> = product.getRelationships(RelationshipType { Relations.PRODUCT_HAS_MAIN_BRAND.toString() }, Direction.INCOMING)
            oldRel.forEach {
                it.delete()
            }

            db.execute(query).use({ result ->
                while (result.hasNext()) {
                    val row = result.next()

                    val brand: Node = row.get("brand") as Node
                    @Suppress("UNCHECKED_CAST")
                    val brandDetect: Iterable<Node> = row.get("brandDetect") as Iterable<Node>
                    var count: Int = 0

                    brandDetect.forEach {
                        val wordCount: Int = wordCount(productContent, it.getProperty("Title").toString())

                        val productToBrandDetectRel: Relationship = product.createRelationshipTo(it, RelationshipType { Relations.BRANDDETECT_HAS_PRODUCT.toString() })
                        productToBrandDetectRel.setProperty("DetectCount", wordCount)

                        count += wordCount
                    }

                    val rel: Relationship = product.createRelationshipTo(brand, RelationshipType { Relations.PRODUCT_HAS_BRAND.toString() })
                    rel.setProperty("DetectCount", count)

                    val detectInstanse = DetectCountViewModel(count, brand)
                    brandDetectCountList.add(detectInstanse)
                }
                result.close()
            })

            val maxBrandDetectElemnt: DetectCountViewModel? = brandDetectCountList.maxBy { it.count }

            if (maxBrandDetectElemnt != null) {

                maxBrandDetectElemnt.detect.createRelationshipTo(product, RelationshipType { Relations.PRODUCT_HAS_MAIN_BRAND.toString() })
            }

            return maxBrandDetectElemnt?.detect
        } catch (e: IllegalArgumentException) {
            log.info("Error: ${e.message.toString()}")
            return null
        }
    }

    private fun defineProductCategory(hashTitle: String): Node? {
        try {
            val product: Node = db.findNode(EngineLable.productLabel(), "HashTitle", hashTitle)
//            val productContent: String = "${product.getProperty("Description")} ${product.getProperty("FaTitle")} ${product.getProperty("EnTitle")}".toLowerCase()

            val query = """MATCH (p:Product {HashTitle:$hashTitle} )-[:${Relations.PRODUCT_HAS_DESCRIPTION}|:${Relations.PRODUCT_HAS_PERSIANTITLE}|:${Relations.PRODUCT_HAS_ENGLISHTITLE}]-(:Word)-[:${Relations.NEXT}*]-(w:Word)
                           WITH w, p
                           MATCH (s:SiteConfiguration)-[:${Relations.RP_CATEGORY_IN_SITE}]-(rc:RPCategory)-[:${Relations.CATEGORYDETECT_IN_RPCATEGORY}]-(cd:CategoryDetect)
                           WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b
                           WITH split(cd.Title," ") as spliteCd, rc, cd, w, p
                           WHERE all(x in spliteCd WHERE x in w.Title)
                           WITH count(w) as wordCountRc,rc as cat,collect(DISTINCT cd.Id) as detectRc, p, w
                           MATCH (cd2:CategoryDetect)
                           WHERE cd2.Id in detectRc
                           CREATE UNIQUE (p)-[:${Relations.CATEGORYDETECT_HAS_PRODUCT}]-(cd2)
                           CREATE UNIQUE (p)-[:${Relations.RP_CATEGORY_HAS_PRODUCT} {DetectCount:wordCount}]-(cat)
                           WITH max(wordCountRc) as maxWordCount,cat,p limit 1
                           CREATE UNIQUE (cat)-[:${Relations.PRODUCT_HAS_MAIN_RPCATEGORY} {DetectCount:wordCount}]-(p)
                           WITH cat,p
                           MATCH (s:SiteConfiguration)-[:${Relations.RP_CATEGORY_IN_SITE.toString()}]-(rp:RPCategory)-[:${Relations.FACT_HAS_CATEGORY.toString()}]-(f:Fact)-[:${Relations.FACTDETECT_IN_FACT}]-(fd:FactDetect)
                           WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                           AND rp.Id = cat.Id
                           WITH split(fd.Title," ") as d2,f,fd
                           WHERE all(x in d2 WHERE x in w.Title)""".trimMargin()

//            val query: String = """MATCH (s:SiteConfiguration)-[:${Relations.RP_CATEGORY_IN_SITE.toString()}]-(c:RPCategory)-[:${Relations.CATEGORYDETECT_IN_RPCATEGORY.toString()}]-(cd:CategoryDetect)
//                                   WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
//                                   WITH c,cd, '$productContent' as productDescription
//                                   WHERE productDescription CONTAINS toString(' ' + cd.Title +' ')
//                                   RETURN c as category, collect(cd) as categoryDetect""".trimMargin()
//                                   UNION
//                                   MATCH (s:SiteConfiguration)-[:${Relations.RP_CATEGORY_IN_SITE.toString()}]-(c:RPCategory)-[:${Relations.AND_CATEGORYDETECT_IN_RPCATEGORY.toString()}]-(cd:CategoryDetect)
//                                   WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
//                                   WITH c,cd, '$productContent' as productDescription,collect(cd.Title) as detectTitle, collect(cd.Id) as detectId
//                                   WHERE all (x IN detectTitle WHERE productDescription CONTAINS toString(' ' + x +' '))
//                                   WITH c,cd, detectTitle, detectId
//                                   MATCH (s:SiteConfiguration)-[:${Relations.RP_CATEGORY_IN_SITE.toString()}]-(cd2:CategoryDetect)
//                                   WHERE cd2.Id in detectId
//                                   RETURN c as category, collect(cd2) as categoryDetect

            val brandDetectCountList = mutableListOf<DetectCountViewModel>()

            val oldRelCategory: Iterable<Relationship> = product.getRelationships(RelationshipType { Relations.RP_CATEGORY_HAS_PRODUCT.toString() }, Direction.OUTGOING)
            oldRelCategory.forEach {
                it.delete()
            }

            val oldRelCategoryDetect: Iterable<Relationship> = product.getRelationships(RelationshipType { Relations.CATEGORYDETECT_HAS_PRODUCT.toString() }, Direction.OUTGOING)
            oldRelCategoryDetect.forEach {
                it.delete()
            }

            val oldRel: Iterable<Relationship> = product.getRelationships(RelationshipType { Relations.PRODUCT_HAS_MAIN_RPCATEGORY.toString() }, Direction.INCOMING)
            oldRel.forEach {
                it.delete()
            }

//            db.execute(query).use({ result ->
//                while (result.hasNext()) {
//                    val row = result.next()
//
//                    val brand: Node = row.get("category") as Node
//                    @Suppress("UNCHECKED_CAST")
//                    val brandDetect: Iterable<Node> = row.get("categoryDetect") as Iterable<Node>
//
//                    var count: Int = 0
//
//                    brandDetect.forEach {
//                        val wordCount: Int = wordCount(productContent, it.getProperty("Title").toString())
//
//                        val productToBrandDetectRel: Relationship = product.createRelationshipTo(it, RelationshipType { Relations.CATEGORYDETECT_HAS_PRODUCT.toString() })
//                        productToBrandDetectRel.setProperty("DetectCount", wordCount)
//
//                        count += wordCount
//                    }
//
//                    val rel: Relationship = product.createRelationshipTo(brand, RelationshipType { Relations.RP_CATEGORY_HAS_PRODUCT.toString() })
//                    rel.setProperty("DetectCount", count)
//
//                    val detectInstanse = DetectCountViewModel(count, brand)
//                    brandDetectCountList.add(detectInstanse)
//                }
//                result.close()
//            })
            db.execute(query)
            val maxBrandDetectElemnt: DetectCountViewModel? = brandDetectCountList.maxBy { it.count }
            if (maxBrandDetectElemnt != null) {

                maxBrandDetectElemnt.detect.createRelationshipTo(product, RelationshipType { Relations.PRODUCT_HAS_MAIN_RPCATEGORY.toString() })
            }

            return maxBrandDetectElemnt?.detect
        } catch (e: Exception) {
            log.info("Error: ${e.message.toString()}")
            return null
        }
    }

    private fun defineProductFact(hashTitle: String, category: Node?): Boolean {
        try {
            if (category != null) {

                val product: Node = db.findNode(EngineLable.productLabel(), "HashTitle", hashTitle)
                val productContent: String = "${product.getProperty("Description")} ${product.getProperty("FaTitle")} ${product.getProperty("EnTitle")}".toLowerCase()

                val query: String = """MATCH (s:SiteConfiguration)-[:${Relations.RP_CATEGORY_IN_SITE.toString()}]-(rp:RPCategory)-[:${Relations.FACT_HAS_CATEGORY.toString()}]-(c:Fact)-[:${Relations.FACTDETECT_IN_FACT}]-(cd:FactDetect)
                                   WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                                   AND rp.Id = '${category.getProperty("Id").toString()}'
                                   WITH c,cd, '$productContent' as productDescription
                                   WHERE productDescription CONTAINS toString(' ' + cd.Title +' ')
                                   RETURN c as fact, collect(cd) as factDetect""".trimMargin()
//                                   UNION
//                                   MATCH (s:SiteConfiguration)-[:${Relations.RP_CATEGORY_IN_SITE.toString()}]-(rp:RPCategory)-[:${Relations.FACT_HAS_CATEGORY.toString()}]-(c:Fact)-[:${Relations.AND_FACTDETECT_IN_FACT}]-(cd:FactDetect)
//                                   WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
//                                   AND rp.Id = '${category.getProperty("Id").toString()}'
//                                   WITH c,cd, '$productContent' as productDescription,collect(cd.Title) as detectTitle, collect(cd.Id) as detectId
//                                   WHERE all (x IN detectTitle WHERE productDescription CONTAINS toString(' ' + x +' '))
//                                   WITH c,cd, detectTitle, detectId
//                                   MATCH (s:SiteConfiguration)-[:${Relations.FACTDETECT_IN_SITE.toString()}]-(cd2:FactDetect)
//                                   WHERE cd2.Id in detectId
//                                   RETURN c as fact, collect(cd2) as factDetect

                val brandDetectCountList = mutableListOf<DetectCountViewModel>()

                val oldRelFact: Iterable<Relationship> = product.getRelationships(RelationshipType { Relations.FACT_HAS_PRODUCT.toString() }, Direction.OUTGOING)
                oldRelFact.forEach {
                    it.delete()
                }

                val oldRelFactDetect: Iterable<Relationship> = product.getRelationships(RelationshipType { Relations.FACTDETECT_HAS_PRODUCT.toString() }, Direction.OUTGOING)
                oldRelFactDetect.forEach {
                    it.delete()
                }

                db.execute(query).use({ result ->
                    while (result.hasNext()) {
                        val row = result.next()

                        val brand: Node = row.get("fact") as Node
                        @Suppress("UNCHECKED_CAST")
                        val brandDetect: Iterable<Node> = row.get("factDetect") as Iterable<Node>
                        var count: Int = 0

                        brandDetect.forEach {
                            val wordCount: Int = wordCount(productContent, it.getProperty("Title").toString())

                            val productToBrandDetectRel: Relationship = product.createRelationshipTo(it, RelationshipType { Relations.FACTDETECT_HAS_PRODUCT.toString() })
                            productToBrandDetectRel.setProperty("DetectCount", wordCount)

                            log.info("word")

                            count += wordCount
                        }

                        val rel: Relationship = product.createRelationshipTo(brand, RelationshipType { Relations.FACT_HAS_PRODUCT.toString() })
                        rel.setProperty("DetectCount", count)

                        val detectInstanse = DetectCountViewModel(count, brand)
                        brandDetectCountList.add(detectInstanse)
                    }
                    result.close()
                })

                return true
            } else
                return false
        } catch (e: Exception) {
            log.info("Error: ${e.message.toString()}")
            return false
        }
    }

    private fun analyseProduct(product: Node): Boolean {

        try {

//            deleteProductRelation(product)

            var query = """MATCH (p:Product {HashTitle:'${product.getProperty("HashTitle")}'} )-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(:Word)-[:NEXT*]-(w:Word)
                           WITH w, p
                           MATCH (s:SiteConfiguration)-[:RP_CATEGORY_IN_SITE]-(c:RPCategory)-[:CATEGORYDETECT_IN_RPCATEGORY]-(cd:CategoryDetect)
                           WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                           WITH split(cd.Title," ") as d, c, cd, collect(w.Title) as word, p
                           WHERE ALL(x in d WHERE x in word)
                          // WITH count(w) as wordCount,c as cat,collect(DISTINCT cd.Id) as detect, p
                          WITH adt.counter(cd.Title,toString(p.Description + " " + p.FaTitle + " " + p.EnTitle)) as dd, cd, c, p
                          WITH sum(dd) as wordCount, collect(cd.Id) as detect, c as cat, p
                           MATCH (cd2:CategoryDetect)
                           WHERE cd2.Id in detect
                           WITH cd2, p, cat, wordCount
                           OPTIONAL MATCH (p)-[r:CATEGORYDETECT_HAS_PRODUCT]-(:CategoryDetect)
                           DELETE r
                           WITH cd2, p, cat, wordCount
                           OPTIONAL MATCH (p)-[r2:RP_CATEGORY_HAS_PRODUCT]-(:RPCategory)
                           DELETE r2
                           WITH cd2, p, cat, wordCount
                           OPTIONAL MATCH (p)<-[r3:PRODUCT_HAS_MAIN_RPCATEGORY]-(:RPCategory)
                           DELETE r3
                           WITH cd2, p, cat, wordCount
                           MERGE (p)-[:CATEGORYDETECT_HAS_PRODUCT]-(cd2)
                           MERGE (p)-[:RP_CATEGORY_HAS_PRODUCT {DetectCount:wordCount}]-(cat)
                           WITH wordCount, cat, p  order by wordCount DESC limit 1
                           MERGE (cat)-[:PRODUCT_HAS_MAIN_RPCATEGORY {DetectCount:wordCount}]-(p)
                           """.trimMargin()

            var queryFact = """
                           MATCH (p:Product {HashTitle:'${product.getProperty("HashTitle")}'})-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(:Word)-[:NEXT*]-(w2:Word)
                           WITH w2, p
                           MATCH (p)-[:PRODUCT_HAS_MAIN_RPCATEGORY]-(rp:RPCategory)-[:FACT_HAS_CATEGORY]-(c2:Fact)-[:FACTDETECT_HAS_FACT]-(cd2:FactDetect)
                           WITH split(cd2.Title," ") as d3, c2, cd2, collect(w2.Title) as word, p
                           WHERE ALL(x in d3 WHERE x in word)
                           WITH adt.counter(cd2.Title,toString(p.Description + " " + p.FaTitle + " " + p.EnTitle)) as dd, cd2, c2, p
                           WITH sum(dd) as wordCount2, collect(cd2.Id) as detect2, c2 as cat2, p
                           //WITH split(cd2.Title," ") as d2, c2, cd2, p, w2
                           //WHERE all(x in d2 WHERE x in w2.Title)
                           //WITH count(w2) as wordCount2, c2 as cat2, collect(DISTINCT cd2.Id) as detect2, p
                           MATCH (cd3:FactDetect)
                           WHERE cd3.Id in detect2
                           WITH cd3, wordCount2, cat2, p
                           OPTIONAL MATCH (p)-[r:FACTDETECT_HAS_PRODUCT]-(:FactDetect)
                           DELETE r
                           WITH cd3, wordCount2, cat2, p
                           OPTIONAL MATCH (p)-[r2:FACT_HAS_PRODUCT]-(:Fact)
                           DELETE r2
                           WITH cd3, wordCount2, cat2, p
                           MERGE (p)-[:FACTDETECT_HAS_PRODUCT]-(cd3)
                           MERGE (p)-[:FACT_HAS_PRODUCT {DetectCount:wordCount2}]-(cat2)""".trimMargin()

            var queryBrand = """
                           MATCH (p:Product {HashTitle:'${product.getProperty("HashTitle")}'})-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(:Word)-[:NEXT*]-(w3:Word)
                           WITH p, w3
                           MATCH (s3:SiteConfiguration)-[:BRAND_IN_SITE]-(c3:Brand)-[:BRANDDEDETECT_IN_BRAND]-(cd3:BrandDetect)
                           WHERE s3.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                           WITH split(cd3.Title," ") as d3, c3, cd3, collect(w3.Title) as word, p
                           WHERE ALL(x in d3 WHERE x in word)
                           WITH adt.counter(cd3.Title,toString(p.Description + " " + p.FaTitle + " " + p.EnTitle)) as dd, cd3, c3, p
                           WITH sum(dd) as wordCount3, collect(cd3.Id) as detect3, c3 as cat3, p
                           //WITH split(cd3.Title," ") as d3, c3, cd3, w3, p
                           //WHERE all(x in d3 WHERE x in w3.Title)
                           //WITH count(w3) as wordCount3, c3 as cat3, collect(DISTINCT cd3.Id) as detect3, p
                           MATCH (cd4:BrandDetect)
                           WHERE cd4.Id in detect3
                           WITH cd4, p, cat3, wordCount3
                           OPTIONAL MATCH (p)-[r:BRANDDETECT_HAS_PRODUCT]-(:BrandDetect)
                           DELETE r
                           WITH cd4, p, cat3, wordCount3
                           OPTIONAL MATCH (p)-[r2:PRODUCT_HAS_BRAND]-(:Brand)
                           DELETE r2
                           WITH cd4, p, cat3, wordCount3
                           OPTIONAL MATCH (p)<-[r3:PRODUCT_HAS_MAIN_BRAND]-(:Brand)
                           DELETE r3
                           WITH cd4, p, cat3, wordCount3
                           MERGE (p)-[:BRANDDETECT_HAS_PRODUCT]-(cd4)
                           MERGE (p)-[:PRODUCT_HAS_BRAND {DetectCount:wordCount3}]-(cat3)
                           WITH wordCount3, cat3, p order by wordCount3 DESC limit 1
                           MERGE (cat3)-[:PRODUCT_HAS_MAIN_BRAND {DetectCount:wordCount3}]-(p)""".trimMargin()

            db.execute(query)
            db.execute(queryFact)
            db.execute(queryBrand)

            return true
        } catch (e: Exception) {
            log.info("Error: " + e.message)
            return false
        }

    }

    private fun analyseAllProduct(): Boolean {
        try {

            var query = """MATCH (p:Product)-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(:Word)-[:NEXT*]-(w:Word)
                           WITH w, p
                           MATCH (s:SiteConfiguration)-[:RP_CATEGORY_IN_SITE]-(c:RPCategory)-[:CATEGORYDETECT_IN_RPCATEGORY]-(cd:CategoryDetect)
                           WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                           WITH split(cd.Title," ") as d, c, cd, collect(w.Title) as word, p
                           WHERE ALL(x in d WHERE x in word)
                          // WITH count(w) as wordCount,c as cat,collect(DISTINCT cd.Id) as detect, p
                          WITH adt.counter(cd.Title,toString(p.Description + " " + p.FaTitle + " " + p.EnTitle)) as dd, cd, c, p
                          WITH sum(dd) as wordCount, collect(cd.Id) as detect, c as cat, p
                           MATCH (cd2:CategoryDetect)
                           WHERE cd2.Id in detect
                           WITH cd2, p, cat, wordCount
                           OPTIONAL MATCH (p)-[r:CATEGORYDETECT_HAS_PRODUCT]-(:CategoryDetect)
                           DELETE r
                           WITH cd2, p, cat, wordCount
                           OPTIONAL MATCH (p)-[r2:RP_CATEGORY_HAS_PRODUCT]-(:RPCategory)
                           DELETE r2
                           WITH cd2, p, cat, wordCount
                           OPTIONAL MATCH (p)<-[r3:PRODUCT_HAS_MAIN_RPCATEGORY]-(:RPCategory)
                           DELETE r3
                           WITH cd2, p, cat, wordCount
                           MERGE (p)-[:CATEGORYDETECT_HAS_PRODUCT]-(cd2)
                           MERGE (p)-[:RP_CATEGORY_HAS_PRODUCT {DetectCount:wordCount}]-(cat)
                           WITH wordCount, cat, p  order by wordCount DESC limit 1
                           MERGE (cat)-[:PRODUCT_HAS_MAIN_RPCATEGORY {DetectCount:wordCount}]-(p)
                           """.trimMargin()

            var queryFact = """
                           MATCH (p:Product)-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(:Word)-[:NEXT*]-(w2:Word)
                           WITH w2, p
                           MATCH (p)-[:PRODUCT_HAS_MAIN_RPCATEGORY]-(rp:RPCategory)-[:FACT_HAS_CATEGORY]-(c2:Fact)-[:FACTDETECT_IN_FACT]-(cd2:FactDetect)
                           WITH split(cd2.Title," ") as d3, c2, cd2, collect(w2.Title) as word, p
                           WHERE ALL(x in d3 WHERE x in word)
                           WITH adt.counter(cd2.Title,toString(p.Description + " " + p.FaTitle + " " + p.EnTitle)) as dd, cd2, c2, p
                           WITH sum(dd) as wordCount2, collect(cd2.Id) as detect2, c2 as cat2, p
                           //WITH split(cd2.Title," ") as d2, c2, cd2, p, w2
                           //WHERE all(x in d2 WHERE x in w2.Title)
                           //WITH count(w2) as wordCount2, c2 as cat2, collect(DISTINCT cd2.Id) as detect2, p
                           MATCH (cd3:FactDetect)
                           WHERE cd3.Id in detect2
                           WITH cd3, wordCount2, cat2, p
                           OPTIONAL MATCH (p)-[r:FACTDETECT_HAS_PRODUCT]-(:FactDetect)
                           DELETE r
                           WITH cd3, wordCount2, cat2, p
                           OPTIONAL MATCH (p)-[r2:FACT_HAS_PRODUCT]-(:Fact)
                           DELETE r2
                           WITH cd3, wordCount2, cat2, p
                           MERGE (p)-[:FACTDETECT_HAS_PRODUCT]-(cd3)
                           MERGE (p)-[:FACT_HAS_PRODUCT {DetectCount:wordCount2}]-(cat2)""".trimMargin()

            var queryBrand = """
                           MATCH (p:Product)-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(:Word)-[:NEXT*]-(w3:Word)
                           WITH p, w3
                           MATCH (s3:SiteConfiguration)-[:BRAND_IN_SITE]-(c3:Brand)-[:BRANDDEDETECT_IN_BRAND]-(cd3:BrandDetect)
                           WHERE s3.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                           WITH split(cd3.Title," ") as d3, c3, cd3, collect(w3.Title) as word, p
                           WHERE ALL(x in d3 WHERE x in word)
                           WITH adt.counter(cd3.Title,toString(p.Description + " " + p.FaTitle + " " + p.EnTitle)) as dd, cd3, c3, p
                           WITH sum(dd) as wordCount3, collect(cd3.Id) as detect3, c3 as cat3, p
                           //WITH split(cd3.Title," ") as d3, c3, cd3, w3, p
                           //WHERE all(x in d3 WHERE x in w3.Title)
                           //WITH count(w3) as wordCount3, c3 as cat3, collect(DISTINCT cd3.Id) as detect3, p
                           MATCH (cd4:BrandDetect)
                           WHERE cd4.Id in detect3
                           WITH cd4, p, cat3, wordCount3
                           OPTIONAL MATCH (p)-[r:BRANDDETECT_HAS_PRODUCT]-(:BrandDetect)
                           DELETE r
                           WITH cd4, p, cat3, wordCount3
                           OPTIONAL MATCH (p)-[r2:PRODUCT_HAS_BRAND]-(:Brand)
                           DELETE r2
                           WITH cd4, p, cat3, wordCount3
                           OPTIONAL MATCH (p)<-[r3:PRODUCT_HAS_MAIN_BRAND]-(:Brand)
                           DELETE r3
                           WITH cd4, p, cat3, wordCount3
                           MERGE (p)-[:BRANDDETECT_HAS_PRODUCT]-(cd4)
                           MERGE (p)-[:PRODUCT_HAS_BRAND {DetectCount:wordCount3}]-(cat3)
                           WITH wordCount3, cat3, p order by wordCount3 DESC limit 1
                           MERGE (cat3)-[:PRODUCT_HAS_MAIN_BRAND {DetectCount:wordCount3}]-(p)""".trimMargin()

            db.execute(query)
            db.execute(queryFact)
            db.execute(queryBrand)
            return true
        } catch (e: Exception) {
            log.info("Error: " + e.message)
            return false
        }
    }

    fun wordCount(s: String, pattern: String): Int {

        var sTemp = s
        var counter = 0

        while (sTemp.length > 0) {
            val index = sTemp.indexOf(pattern)
            if (index == -1) break
            sTemp = sTemp.substring(index + pattern.length, sTemp.length)
            counter++
        }
        return counter
    }

    fun deleteProductRelation(product: Node) {


        var queryDelete = """MATCH (p:Product {HashTitle:"${product.getProperty("HashTitle")}"})-[r1:RP_CATEGORY_HAS_PRODUCT|:PRODUCT_HAS_BRAND|:PRODUCT_HAS_FACT|:BRANDDETECT_HAS_PRODUCT|:CATEGORYDETECT_HAS_PRODUCT|:FACTDETECT_HAS_PRODUCT|:PRODUCT_HAS_MAIN_RPCATEGORY|:PRODUCT_HAS_MAIN_BRAND]-(d)
                             DELETE r1""".trimMargin()

        db.execute(queryDelete)
    }

}

class DetectCountViewModel(Count: Int, Detect: Node) {

    var detect: Node = Detect
    var count: Int = Count
}

class DetectCount {
    lateinit var detect: Node
    var count: Int = 0
}

class EngineLable {
    companion object {
        fun productLabel(): Label = Label.label("Product")
    }
}

class Product {
    var FaTitle: String = ""
    var EnTitle: String = ""
    var HashTitle: String = ""
    var Price: String = ""
    var ImagePath: String = ""
    var Description: String = ""
    var CreatedDate = null
}