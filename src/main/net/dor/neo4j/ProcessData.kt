package dor.neo4j

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
import org.neo4j.graphdb.QueryExecutionType.query
import javax.management.relation.Relation
import javax.swing.text.StyledEditorKit


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

    @UserFunction(name = "adt.countDescription")
    fun countProductDesc(@Name("a") pattern: String, @Name("a") description: String): String {
        var sTemp = description
        var counter = 0

        while (sTemp.length > 0) {
            val index = sTemp.indexOf(pattern)
            if (index == -1) break
            sTemp = sTemp.substring(index + pattern.length, sTemp.length)
            counter++
        }
        return counter.toString()
    }

    @Procedure(name = "adt.createProduct")
    fun createProduct() {
        val q = "Create (c:Product { product })\n" +
                "With c\n" +
                "Match (rs:RS)\n" +
                "Where rs.SiteId = \"50cfc9e8-402b-495b-8ed4-66dcb2b3aadd\"\n" +
                "Create Unique (rs)<-[:${Relations.PRODUCT_IN_RS.toString()}]-(c)"
        db.execute(q)
    }

    @Procedure(name = "adt.defineProduct", mode = Mode.WRITE)
    fun defineProduct(@Name("HashTitle") hashTitle: String) {
        defineProductBrand(hashTitle)
        val Category = defineProductCategory(hashTitle)
        defineProductFact(hashTitle, Category)
    }

    @Procedure(name = "adt.defineAllProducts", mode = Mode.WRITE)
    fun defineAllProducts() {
        val productNodes: ResourceIterator<Node> = db.findNodes(EngineLable.productLabel())
        val startTime = System.currentTimeMillis()

        productNodes.forEach {
            defineProductBrand(it.getProperty("HashTitle").toString())
            val cat = defineProductCategory(it.getProperty("HashTitle").toString())
            defineProductFact(it.getProperty("HashTitle").toString(), cat)
        }
        val endTime = System.currentTimeMillis()
        val duration = (endTime - startTime)

        log.info(duration.toString())

    }

    private fun defineProductBrand(hashTitle: String): Node? {
        try {
            val product: Node = db.findNode(EngineLable.productLabel(), "HashTitle", hashTitle)
            val productContent: String = "${product.getProperty("Description")} ${product.getProperty("FaTitle")} ${product.getProperty("EnTitle")}".toLowerCase()

            val query: String = """MATCH (s:SiteConfiguration)-[:${Relations.BRAND_IN_SITE.toString()}]-(c:Brand)-[:${Relations.BRANDDEDETECT_IN_BRAND.toString()}]-(cd:BrandDetect)
                                   WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                                   WITH c,cd, '$productContent' as productDescription
                                   WHERE productDescription CONTAINS toString(' ' + cd.Title +' ')
                                   RETURN c as brand, collect(cd) as brandDetect
                                   UNION
                                   MATCH (s:SiteConfiguration)-[:${Relations.BRAND_IN_SITE.toString()}]-(c:Brand)-[:${Relations.AND_BRANDDEDETECT_IN_BRAND.toString()}]-(cd:BrandDetect)
                                   WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                                   WITH c,cd, '$productContent' as productDescription,collect(cd.Title) as detectTitle, collect(cd.Id) as detectId
                                   WHERE all (x IN detectTitle WHERE productDescription CONTAINS toString(' ' + x +' '))
                                   WITH c,cd, detectTitle, detectId
                                   MATCH (s:SiteConfiguration)-[:${Relations.BRANDDETECT_IN_SITE.toString()}]-(cd2:BrandDetect)
                                   WHERE cd2.Id in detectId
                                   RETURN c as brand, collect(cd2) as brandDetect""".trimMargin()

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
                    log.info(detectInstanse.toString())
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
            val productContent: String = "${product.getProperty("Description")} ${product.getProperty("FaTitle")} ${product.getProperty("EnTitle")}".toLowerCase()

            val query: String = """MATCH (s:SiteConfiguration)-[:${Relations.RP_CATEGORY_IN_SITE.toString()}]-(c:RPCategory)-[:${Relations.CATEGORYDETECT_IN_RPCATEGORY.toString()}]-(cd:CategoryDetect)
                                   WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                                   WITH c,cd, '$productContent' as productDescription
                                   WHERE productDescription CONTAINS toString(' ' + cd.Title +' ')
                                   RETURN c as category, collect(cd) as categoryDetect
                                   UNION
                                   MATCH (s:SiteConfiguration)-[:${Relations.RP_CATEGORY_IN_SITE.toString()}]-(c:RPCategory)-[:${Relations.AND_CATEGORYDETECT_IN_RPCATEGORY.toString()}]-(cd:CategoryDetect)
                                   WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                                   WITH c,cd, '$productContent' as productDescription,collect(cd.Title) as detectTitle, collect(cd.Id) as detectId
                                   WHERE all (x IN detectTitle WHERE productDescription CONTAINS toString(' ' + x +' '))
                                   WITH c,cd, detectTitle, detectId
                                   MATCH (s:SiteConfiguration)-[:${Relations.RP_CATEGORY_IN_SITE.toString()}]-(cd2:CategoryDetect)
                                   WHERE cd2.Id in detectId
                                   RETURN c as category, collect(cd2) as categoryDetect""".trimMargin()

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

            db.execute(query).use({ result ->
                while (result.hasNext()) {
                    val row = result.next()

                    val brand: Node = row.get("category") as Node
                    @Suppress("UNCHECKED_CAST")
                    val brandDetect: Iterable<Node> = row.get("categoryDetect") as Iterable<Node>

                    var count: Int = 0

                    brandDetect.forEach {
                        val wordCount: Int = wordCount(productContent, it.getProperty("Title").toString())

                        val productToBrandDetectRel: Relationship = product.createRelationshipTo(it, RelationshipType { Relations.CATEGORYDETECT_HAS_PRODUCT.toString() })
                        productToBrandDetectRel.setProperty("DetectCount", wordCount)

                        count += wordCount
                    }

                    val rel: Relationship = product.createRelationshipTo(brand, RelationshipType { Relations.RP_CATEGORY_HAS_PRODUCT.toString() })
                    rel.setProperty("DetectCount", count)

                    val detectInstanse = DetectCountViewModel(count, brand)
                    brandDetectCountList.add(detectInstanse)
                }
                result.close()
            })

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
                                   RETURN c as fact, collect(cd) as factDetect
                                   UNION
                                   MATCH (s:SiteConfiguration)-[:${Relations.RP_CATEGORY_IN_SITE.toString()}]-(rp:RPCategory)-[:${Relations.FACT_HAS_CATEGORY.toString()}]-(c:Fact)-[:${Relations.AND_FACTDETECT_IN_FACT}]-(cd:FactDetect)
                                   WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                                   AND rp.Id = '${category.getProperty("Id").toString()}'
                                   WITH c,cd, '$productContent' as productDescription,collect(cd.Title) as detectTitle, collect(cd.Id) as detectId
                                   WHERE all (x IN detectTitle WHERE productDescription CONTAINS toString(' ' + x +' '))
                                   WITH c,cd, detectTitle, detectId
                                   MATCH (s:SiteConfiguration)-[:${Relations.FACTDETECT_IN_SITE.toString()}]-(cd2:FactDetect)
                                   WHERE cd2.Id in detectId
                                   RETURN c as fact, collect(cd2) as factDetect""".trimMargin()

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


}

class DetectCountViewModel(Count: Int, Detect: Node) {

    var detect: Node = Detect
    var count: Int = Count
}

class EngineLable {
    companion object {
        fun productLabel(): Label = Label.label("Product")
    }
}