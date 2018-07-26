package dor.neo4j

import jdk.nashorn.internal.parser.JSONParser
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
import java.util.UUID


private class Sha256Hash {
    fun hash(data: String): String {
        val bytes = data.toByteArray()
        val md = MessageDigest.getInstance("SHA-256")
        val digest = md.digest(bytes)
        return digest.fold("", { str, it -> str + "%02x".format(it) })
    }

}


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

    //val RSId = "50cfc9e8-402b-495b-8ed4-66dcb2b3aadd"


    @Context
    lateinit var db: GraphDatabaseService


    @Context
    lateinit var log: Log

    @UserFunction(name = "dor.uuid")
    @Description("creates an UUID (universally unique id)")
    fun CreateUUID(): String {
        return UUID.randomUUID().toString()
    }

    @UserFunction(name = "dor.sha256")
    @Description("Convert data from string to Sha256 String in a function")
    fun Sha256Maker(@Name("data") data: String): String {
        // Note : Neo4j UserFunction only accepts a limited range of types.
        val h = Sha256Hash()
        return h.hash(data)
    }

    @UserFunction(name = "dor.replace")
    @Description("replace And Normalize Text for Analyse")
    fun replace(@Name("Text") text: String): String {

        val re = Regex("(?<!\\d)\\.(?!\\d)")
        var tempText: String
        tempText = re.replace(text, "")
        tempText = tempText.toLowerCase().trim().replace("’", "").replace("'", "").replace("[", "").replace("]", "").replace("(", "").replace(")", "").replace("{", "").replace("}", "").replace("⟨", "").replace("⟩", "").replace(":", "").replace(",", "").replace("،", "").replace("、", "").replace("‒", "").replace("–", "").replace("—", "").replace("―", "").replace("…", "").replace("...", "").replace("⋯", "").replace("᠁", "").replace("ฯ", "").replace("!", "").replace("‹", "").replace("›", "").replace("«", "").replace("»", "").replace("‐", "").replace("-", "").replace("?", "").replace("‘", "").replace("’", "").replace("“", "").replace("”", "").replace("'", "").replace("'", "").replace("\"", "").replace(";", "").replace("/", "").replace("·", "").replace("&", "").replace("*", "").replace("@", "").replace("\\", "").replace("•", "").replace(" ^ ", "").replace("°", "").replace("”", "").replace("#", "").replace("÷", "").replace("×", "").replace("º", "").replace("ª", "").replace("%", "").replace("‰", "").replace("+", "").replace("−", "").replace("=", "").replace("‱", "").replace("¶", "").replace("′", "").replace("″", "").replace("‴", "").replace("§", "").replace("~", "").replace("_", "").replace("|", "").replace("‖", "").replace("¦", "").replace("©", "").replace("℗", "").replace("®", "").replace("℠", "").replace("،", "").replace("؟", "").replace("»", "").replace("«", "").replace("؛", "").replace("-", "").replace("...", "").replace("ً", "").replace("ٌ", "").replace("ٍ", "").replace("َ", "").replace("ُ", "").replace("ِ", "").replace("  ", " ").replace("ي", "ی").replace("ك", "ک").replace("1", "۱").replace("2", "۲").replace("3", "۳").replace("4", "۴").replace("5", "۵").replace("6", "۶").replace("7", "۷").replace("8", "۸").replace("9", "۹").replace("0", "۰")
        return tempText
    }


    @UserFunction(name = "dor.counter")
    fun counter(@Name("Detect") pattern: String, @Name("Description") text: String): Number {
        var sTemp = text.toLowerCase()
        var counter = 0
        val sPattern = " " + pattern + " "

        while (sTemp.length > 0) {
            val index = sTemp.indexOf(sPattern)
            if (index == -1) break
            sTemp = sTemp.substring(index + sPattern.length, sTemp.length)
            counter++
        }
        return counter
    }

    @Procedure(name = "dor.createProduct", mode = Mode.WRITE)
    fun createProduct(@Name("FaTitle") FaTitle: String, @Name("EnTitle") EnTitle: String, @Name("Description") Description: String, @Name("Price") Price: Long, @Name("SourceURL") SourceUrl: String, @Name("ImagePath") ImagePath: String, @Name("Spec") Spec: String) {

        try {
            val hashFaTitle = Sha256Maker(FaTitle)
            val product: Node? = db.findNode(EngineLable.productLabel(), "HashTitle", hashFaTitle)
            val site: Node? = db.findNode(EngineLable.rsLabel(), "SiteId", "50cfc9e8-402b-495b-8ed4-66dcb2b3aadd")

            if (product == null) {
                var flag = false
                val tempDesc = "${Description.trim()} ${FaTitle.trim()} ${EnTitle.trim()} ${Spec.trim()}"
                val query = """
                        //--------start match product and words--------
                        WITH "$tempDesc" as w1
                        WITH dor.replace(w1) as w
                        WITH SPLIT(w, " ") as word
                        WITH reduce(v='|', x in word | v + x + '|') as arrayAsString
                        //--------end match product and words-------
                        //--------start match category by specdetect-------
                        MATCH (s:SiteConfiguration)-[:SPECDETECT_IN_SITE]-(spd:SpecDetect)-[:SPECDETECT_IN_SPEC]-(spec:Spec)
                        WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                        WITH split(spd.Title," ") as splitSpd, spd, arrayAsString
                        WITH splitSpd, reduce(v='|', x in splitSpd | v + x + '|') as testAsString, spd, arrayAsString
                        WHERE arrayAsString CONTAINS testAsString
                        RETURN spd""".trimMargin()

                val result = db.execute(query)

                if (result.hasNext()) {
                    flag = true
                }

                if (flag) {
                    val id = CreateUUID()

                    val p = db.createNode(EngineLable.productLabel())
                    p.setProperty("FaTitle", FaTitle)
                    p.setProperty("EnTitle", EnTitle)
                    p.setProperty("Description", Description)
                    p.setProperty("Spec", Spec)
                    p.setProperty("Price", Price)
                    p.setProperty("SourceUrl", SourceUrl)
                    p.setProperty("ImagePath", ImagePath)
                    p.setProperty("HashTitle", hashFaTitle)
                    p.setProperty("Id", id)
                    p.createRelationshipTo(site, RelationshipType { Relations.PRODUCT_IN_RS.toString() })

                    analyseProduct(p)
//            var tempDesc = Description.toLowerCase().trim().replace("’", "").replace("'", "").replace("[", "").replace("]", "").replace("(", "").replace(")", "").replace("{", "").replace("}", "").replace("⟨", "").replace("⟩", "").replace(":", "").replace(",", "").replace("،", "").replace("、", "").replace("‒", "").replace("–", "").replace("—", "").replace("―", "").replace("…", "").replace("...", "").replace("⋯", "").replace("᠁", "").replace("ฯ", "").replace("!", "").replace("‹", "").replace("›", "").replace("«", "").replace("»", "").replace("‐", "").replace("-", "").replace("?", "").replace("‘", "").replace("’", "").replace("“", "").replace("”", "").replace("'", "").replace("'", "").replace("\"", "").replace(";", "").replace("/", "").replace("·", "").replace("&", "").replace("*", "").replace("@", "").replace("\\", "").replace("•", "").replace(" ^ ", "").replace("°", "").replace("”", "").replace("#", "").replace("÷", "").replace("×", "").replace("º", "").replace("ª", "").replace("%", "").replace("‰", "").replace("+", "").replace("−", "").replace("=", "").replace("‱", "").replace("¶", "").replace("′", "").replace("″", "").replace("‴", "").replace("§", "").replace("~", "").replace("_", "").replace("|", "").replace("‖", "").replace("¦", "").replace("©", "").replace("℗", "").replace("®", "").replace("℠", "").replace("،", "").replace("؟", "").replace("»", "").replace("«", "").replace("؛", "").replace("-", "").replace("...", "").replace("ً", "").replace("ٌ", "").replace("ٍ", "").replace("َ", "").replace("ُ", "").replace("ِ", "")
//            val descQuery = """
//                   //Description Chain
//                   MATCH (c:Product)
//                   WHERE c.Id = "$id"
//                   WITH split("$tempDesc"," ") as words, c
//                   UNWIND range(0,size(words)-2) as i
//                   MERGE (w3:Word {Title:words[i],Hash:dor.sha256(words[i]+i+c.Id)})
//                   MERGE (w4:Word {Title:words[i+1],Hash:dor.sha256(words[i+1]+(i+1)+c.Id)})
//                   CREATE (w3)-[:NEXT]->(w4)
//                   WITH c, words
//                   MATCH (w:Word {Hash:dor.sha256(words[0]+0+c.Id)})
//                   MERGE (c)-[:PRODUCT_HAS_DESCRIPTION]->(w)""".trimMargin()
//

                }

            } else {
                analyseProduct(product)
            }

        } catch (e: Exception) {
            log.info(e.message.toString())
        }

    }

    @Procedure(name = "dor.defineProduct", mode = Mode.WRITE)
    fun defineProduct(@Name("HashTitle") hashTitle: String) {

        val product: Node = db.findNode(EngineLable.productLabel(), "HashTitle", hashTitle)
        analyseProduct(product)

    }

    @Procedure(name = "dor.defineAllProducts", mode = Mode.WRITE)
    fun defineAllProducts() {
        try {

            analyseAllProduct()

        } catch (e: Exception) {
            log.info("${e.message}")
        }


    }

    private fun analyseProduct(product: Node): Boolean {

        try {

            var queryDelete = """
                MATCH (product:Product {HashTitle:'${product.getProperty("HashTitle")}'})
                OPTIONAL MATCH (product)-[r:CATEGORYDETECT_HAS_PRODUCT]-(:CategoryDetect)
                OPTIONAL MATCH (product)-[r2:RP_CATEGORY_HAS_PRODUCT]-(:RPCategory)
                OPTIONAL MATCH (product)<-[r3:PRODUCT_HAS_MAIN_RPCATEGORY]-(:RPCategory)
                OPTIONAL MATCH (product)-[r4:SPECDETECT_HAS_PRODUCT]-(:SpecDetect)
                OPTIONAL MATCH (product)-[r5:FACTDETECT_HAS_PRODUCT]-(:FactDetect)
                OPTIONAL MATCH (product)-[r6:FACT_HAS_PRODUCT]-(:Fact)
                OPTIONAL MATCH (product)-[r7:BRANDDETECT_HAS_PRODUCT]-(:BrandDetect)
                OPTIONAL MATCH (product)-[r8:PRODUCT_HAS_BRAND]-(:Brand)
                OPTIONAL MATCH (product)<-[r9:PRODUCT_HAS_MAIN_BRAND]-(:Brand)
                DELETE r,r2,r3,r4,r5,r6,r7,r8,r9 """.trimMargin()

//                       val query = """
//                        //--------start match product and words--------
//                        MATCH (product:Product {HashTitle:'${product.getProperty("HashTitle")}'})-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(ww:Word)
//                        WITH ww, product
//                        CALL apoc.path.subgraphNodes(ww,{ relationshipFilter:'NEXT>', labelFilter:'Word', filterStartNode:true, limit:-1}) yield node as w
//                        WITH collect(w.Title) as word, product
//                        WITH reduce(v='|', x in word | v + x + '|') as arrayAsString, product
//                        //--------end match product and words-------
//                        //--------start match category by specdetect-------
//                        MATCH (s:SiteConfiguration)-[:SPECDETECT_IN_SITE]-(spd:SpecDetect)-[:SPECDETECT_IN_SPEC]-(spec:Spec)
//                        WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
//                        WITH split(spd.Title," ") as splitSpd, spd, arrayAsString, product
//                        WITH splitSpd, reduce(v='|', x in splitSpd | v + x + '|') as testAsString, spd, product, arrayAsString
//                        WHERE arrayAsString CONTAINS testAsString
//                        WITH DISTINCT spd, product, arrayAsString
//                        MATCH (spd)-[:SPECDETECT_IN_SPEC]-(:Spec)-[:SPEC_HAS_CATEGORY]-(rp:RPCategory)
//                        WITH collect(spd.Id) as detect3, rp, product, arrayAsString
//                        //--------end match category by specdetect-------
//                        //--------start match category by categorydetect-------
//                        MATCH (rp)-[:RPC_IS_RPC_CHILD*0..]->(rp2:RPCategory)-[:CATEGORYDETECT_IN_RPCATEGORY]-(cd:CategoryDetect)
//                        WITH split(cd.Title," ") as splitCd, cd, rp2, detect3, arrayAsString, product
//                        WITH splitCd, reduce(v='|', x in splitCd | v + x + '|') as testAsString, cd, rp2, product, detect3, arrayAsString
//                        WHERE arrayAsString CONTAINS testAsString
//                        WITH length(split(replace(arrayAsString,testAsString,"@"), "@")) -1 as dd, cd, rp2 , product, detect3, arrayAsString
//                        //--------end match category by categorydetect-------
//                        //--------start create category and spec rels-------
//                        MERGE (product)-[:CATEGORYDETECT_HAS_PRODUCT {DetectCount:dd}]-(cd)
//                        WITH sum(dd) as sumCd, rp2, product, detect3, arrayAsString
//                        MERGE (product)-[:RP_CATEGORY_HAS_PRODUCT {DetectCount:sumCd}]-(rp2)
//                        WITH sumCd, rp2, product, detect3, arrayAsString ORDER BY sumCd DESC limit 1
//                        MERGE (rp2)-[:PRODUCT_HAS_MAIN_RPCATEGORY {DetectCount:sumCd}]-(product)
//                        WITH detect3, product, rp2, arrayAsString
//                        MATCH (cd3:SpecDetect)-[:SPECDETECT_IN_SPEC]-(:Spec)-[:SPEC_HAS_CATEGORY]-(rp2)
//                        WHERE cd3.Id IN detect3
//                        MERGE (product)-[r5:SPECDETECT_HAS_PRODUCT]->(cd3)
//                        WITH distinct arrayAsString, product
//                        //--------end create category and spec rels-------
//                        //--------start match brand by branddetect-------
//                        MATCH (s:SiteConfiguration)-[:BRAND_IN_SITE]-(c3:Brand)-[:BRANDDEDETECT_IN_BRAND]-(cd3:BrandDetect)
//                        WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
//                        WITH split(cd3.Title," ") as d3, c3, cd3, arrayAsString, product
//                        WITH d3,reduce(v='|', x in d3 | v + x + '|') as testAsString, cd3, c3, product, arrayAsString
//                        WHERE arrayAsString CONTAINS testAsString
//                        WITH length(split(replace(arrayAsString,testAsString,"@"), "@"))-1 as dd, cd3, c3, product, arrayAsString
//                        MERGE (product)-[:BRANDDETECT_HAS_PRODUCT {DetectCount:dd}]-(cd3)
//                        WITH sum(dd) as wordCount3, c3 as cat3, product, arrayAsString
//                        MERGE (product)-[:PRODUCT_HAS_BRAND {DetectCount:wordCount3}]-(cat3)
//                        WITH wordCount3, cat3, product, arrayAsString order by wordCount3 DESC limit 1
//                        MERGE (cat3)-[:PRODUCT_HAS_MAIN_BRAND {DetectCount:wordCount3}]-(product)
//                        WITH distinct arrayAsString, product
//                        //--------end match brand by branddetect-------
//                        //--------start match fact by factdetect-------
//                        MATCH (s:SiteConfiguration)-[:FACT_IN_SITE]-(c3:Fact)-[:FACTDETECT_IN_FACT]-(cd3:FactDetect)
//                        WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
//                        WITH split(cd3.Title," ") as d3, c3, cd3, arrayAsString, product
//                        WITH d3,reduce(v='|', x in d3 | v + x + '|') as testAsString, cd3, c3, product, arrayAsString
//                        WHERE arrayAsString CONTAINS testAsString
//                        WITH length(split(replace(arrayAsString,testAsString,"@"), "@"))-1 as dd, cd3, c3, product, arrayAsString
//                        MERGE (product)-[:FACTDETECT_HAS_PRODUCT {DetectCount:dd}]-(cd3)
//                        WITH sum(dd) as wordCount3, c3 as cat3, product, arrayAsString
//                        MERGE (product)-[:FACT_HAS_PRODUCT {DetectCount:wordCount3}]-(cat3)
//                        //--------end match fact by factdetect-------""".trimMargin()

//            var queryFact = """
//                           MATCH (p:Product {HashTitle:'${product.getProperty("HashTitle")}'})-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(ww:Word)
//                           WITH p, ww
//                           CALL apoc.path.subgraphNodes(ww,{ relationshipFilter:'NEXT>', labelFilter:'Word', filterStartNode:true, limit:-1}) yield node as w3
//                           WITH w3, p
//                           MATCH (s3:SiteConfiguration)-[:FACT_IN_SITE]-(c3:Fact)-[:FACTDETECT_IN_FACT]-(cd3:FactDetect)
//                           WHERE s3.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
//                           WITH split(cd3.Title," ") as d3, c3, cd3, collect(w3.Title) as word, p
//                           WITH d3,reduce(v='|', x in d3 | v + x + '|') as testAsString, reduce(v='|', x in word | v + x + '|') as arrayAsString, cd3, c3, p
//                           WHERE arrayAsString CONTAINS testAsString
//                           //WITH reduce(t=trim(toLower(p.Description)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalizedDesc, cd3, c3, p
//                           //WITH reduce(t=trim(toLower(p.FaTitle)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalizedFaTitle, cd3, c3, p, normalizedDesc
//                           //WITH reduce(t=trim(toLower(p.EnTitle)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalizedEnTitle, cd3, c3, p, normalizedDesc, normalizedFaTitle
//                           WITH length(split(replace(arrayAsString,testAsString,"@"), "@"))-1 as dd, cd3, c3, p
//                           MERGE (p)-[:FACTDETECT_HAS_PRODUCT {DetectCount:dd}]-(cd3)
//                           //WITH dor.counter(cd3.Title,toString(normalizedDesc + " " + normalizedFaTitle + " " + normalizedEnTitle)) as dd, cd3, c3, p
//                           WITH sum(dd) as wordCount3, c3 as cat3, p
//                           MERGE (p)-[:FACT_HAS_PRODUCT {DetectCount:wordCount3}]-(cat3)""".trimMargin()

            val query = """
                        //--------start match product and words--------
                        MATCH (product:Product {HashTitle:'${product.getProperty("HashTitle")}'})
                        WITH product, dor.replace(toString(product.Description + product.FaTitle + product.EnTitle + product.Spec)) as w
                        WITH SPLIT(w, " ") as word, product
                        WITH reduce(v='|', x in word | v + x + '|') as arrayAsString, product
                        //--------end match product and words-------
                        //--------start match category by specdetect-------
                        MATCH (s:SiteConfiguration)-[:SPECDETECT_IN_SITE]-(spd:SpecDetect)-[:SPECDETECT_IN_SPEC]-(spec:Spec)
                        WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                        WITH split(spd.Title," ") as splitSpd, spd, arrayAsString, product
                        WITH splitSpd, reduce(v='|', x in splitSpd | v + x + '|') as testAsString, spd, product, arrayAsString
                        WHERE arrayAsString CONTAINS testAsString
                        WITH DISTINCT spd, product, arrayAsString
                        MATCH (spd)-[:SPECDETECT_IN_SPEC]-(:Spec)-[:SPEC_HAS_CATEGORY]-(rp:RPCategory)
                        WITH collect(spd.Id) as detect3, rp, product, arrayAsString
                        //--------end match category by specdetect-------
                        //--------start match category by categorydetect-------
                        MATCH (rp)-[:RPC_IS_RPC_CHILD*0..]->(rp2:RPCategory)-[:CATEGORYDETECT_IN_RPCATEGORY]-(cd:CategoryDetect)
                        WITH split(cd.Title," ") as splitCd, cd, rp2, detect3, arrayAsString, product
                        WITH splitCd, reduce(v='|', x in splitCd | v + x + '|') as testAsString, cd, rp2, product, detect3, arrayAsString
                        WHERE arrayAsString CONTAINS testAsString
                        WITH length(split(replace(arrayAsString,testAsString,"@"), "@")) -1 as dd, cd, rp2 , product, detect3, arrayAsString
                        //--------end match category by categorydetect-------
                        //--------start create category and spec rels-------
                        MERGE (product)-[:CATEGORYDETECT_HAS_PRODUCT {DetectCount:dd}]-(cd)
                        WITH sum(dd) as sumCd, rp2, product, detect3, arrayAsString
                        MERGE (product)-[:RP_CATEGORY_HAS_PRODUCT {DetectCount:sumCd}]-(rp2)
                        WITH sumCd, rp2, product, detect3, arrayAsString ORDER BY sumCd DESC limit 1
                        MERGE (rp2)-[:PRODUCT_HAS_MAIN_RPCATEGORY {DetectCount:sumCd}]-(product)
                        WITH detect3, product, rp2, arrayAsString
                        MATCH (cd3:SpecDetect)-[:SPECDETECT_IN_SPEC]-(:Spec)-[:SPEC_HAS_CATEGORY]-(rp2)
                        WHERE cd3.Id IN detect3
                        MERGE (product)-[r5:SPECDETECT_HAS_PRODUCT]->(cd3)
                        //--------end create category and spec rels-------""".trimMargin()

            val queryBrand = """
                          MATCH (product:Product {HashTitle:'${product.getProperty("HashTitle")}'})
                          WITH product, dor.replace(toString(product.Description + product.FaTitle + product.EnTitle + product.Spec)) as w
                          WITH SPLIT(w, " ") as word, product
                          WITH reduce(v='|', x in word | v + x + '|') as arrayAsString, product
                          //--------start match brand by branddetect-------
                          MATCH (s:SiteConfiguration)-[:BRAND_IN_SITE]-(c3:Brand)-[:BRANDDEDETECT_IN_BRAND]-(cd3:BrandDetect)
                          WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                          WITH split(cd3.Title," ") as d3, c3, cd3, arrayAsString, product
                          WITH d3,reduce(v='|', x in d3 | v + x + '|') as testAsString, cd3, c3, product, arrayAsString
                          WHERE arrayAsString CONTAINS testAsString
                          WITH length(split(replace(arrayAsString,testAsString,"@"), "@"))-1 as dd, cd3, c3, product, arrayAsString
                          MERGE (product)-[:BRANDDETECT_HAS_PRODUCT {DetectCount:dd}]-(cd3)
                          WITH sum(dd) as wordCount3, c3 as cat3, product, arrayAsString
                          MERGE (product)-[:PRODUCT_HAS_BRAND {DetectCount:wordCount3}]-(cat3)
                          WITH wordCount3, cat3, product, arrayAsString order by wordCount3 DESC limit 1
                          MERGE (cat3)-[:PRODUCT_HAS_MAIN_BRAND {DetectCount:wordCount3}]-(product)""".trimMargin()

            val queryFact = """
                        MATCH (product:Product {HashTitle:'${product.getProperty("HashTitle")}'})
                        WITH product, dor.replace(toString(product.Description + product.FaTitle + product.EnTitle + product.Spec)) as w
                        WITH SPLIT(w, " ") as word, product
                        WITH reduce(v='|', x in word | v + x + '|') as arrayAsString, product
                        //--------start match fact by factdetect-------
                        MATCH (s:SiteConfiguration)-[:FACT_IN_SITE]-(c3:Fact)-[:FACTDETECT_IN_FACT]-(cd3:FactDetect)
                        WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                        WITH split(cd3.Title," ") as d3, c3, cd3, arrayAsString, product
                        WITH d3,reduce(v='|', x in d3 | v + x + '|') as testAsString, cd3, c3, product, arrayAsString
                        WHERE arrayAsString CONTAINS testAsString
                        WITH length(split(replace(arrayAsString,testAsString,"@"), "@"))-1 as dd, cd3, c3, product, arrayAsString
                        MERGE (product)-[:FACTDETECT_HAS_PRODUCT {DetectCount:dd}]-(cd3)
                        WITH sum(dd) as wordCount3, c3 as cat3, product, arrayAsString
                        MERGE (product)-[:FACT_HAS_PRODUCT {DetectCount:wordCount3}]-(cat3)
                        //--------end match fact by factdetect-------""".trimMargin()

            db.execute(queryDelete)
            db.execute(query)
            db.execute(queryBrand)
            db.execute(queryFact)

            return true
        } catch (e: Exception) {
            log.info("Error: " + e.message)
            return false
        }

    }

    private fun analyseAllProduct(): Boolean {
        try {

            var queryDelete = """
                MATCH (product:Product)
                OPTIONAL MATCH (product)-[r:CATEGORYDETECT_HAS_PRODUCT]-(:CategoryDetect)
                OPTIONAL MATCH (product)-[r2:RP_CATEGORY_HAS_PRODUCT]-(:RPCategory)
                OPTIONAL MATCH (product)<-[r3:PRODUCT_HAS_MAIN_RPCATEGORY]-(:RPCategory)
                OPTIONAL MATCH (product)-[r4:SPECDETECT_HAS_PRODUCT]-(:SpecDetect)
                OPTIONAL MATCH (product)-[r5:FACTDETECT_HAS_PRODUCT]-(:FactDetect)
                OPTIONAL MATCH (product)-[r6:FACT_HAS_PRODUCT]-(:Fact)
                OPTIONAL MATCH (product)-[r7:BRANDDETECT_HAS_PRODUCT]-(:BrandDetect)
                OPTIONAL MATCH (product)-[r8:PRODUCT_HAS_BRAND]-(:Brand)
                OPTIONAL MATCH (product)<-[r9:PRODUCT_HAS_MAIN_BRAND]-(:Brand)
                DELETE r,r2,r3,r4,r5,r6,r7,r8,r9
                """.trimMargin()

            val query = """
                        //--------start match product and words--------
                        MATCH (product:Product)-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(ww:Word)
                        WITH ww, product
                        CALL apoc.path.subgraphNodes(ww,{ relationshipFilter:'NEXT>', labelFilter:'Word', filterStartNode:true, limit:-1}) yield node as w
                        WITH collect(w.Title) as word, product
                        WITH reduce(v='|', x in word | v + x + '|') as arrayAsString, product
                        //--------end match product and words-------
                        //--------start match category by specdetect-------
                        MATCH (s:SiteConfiguration)-[:SPECDETECT_IN_SITE]-(spd:SpecDetect)-[:SPECDETECT_IN_SPEC]-(spec:Spec)
                        WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                        WITH split(spd.Title," ") as splitSpd, spd, arrayAsString, product
                        WITH splitSpd, reduce(v='|', x in splitSpd | v + x + '|') as testAsString, spd, product, arrayAsString
                        WHERE arrayAsString CONTAINS testAsString
                        WITH DISTINCT spd, product, arrayAsString
                        MATCH (spd)-[:SPECDETECT_IN_SPEC]-(:Spec)-[:SPEC_HAS_CATEGORY]-(rp:RPCategory)
                        WITH collect(spd.Id) as detect3, rp, product, arrayAsString
                        //--------end match category by specdetect-------
                        //--------start match category by categorydetect-------
                        MATCH (rp)-[:RPC_IS_RPC_CHILD*0..]->(rp2:RPCategory)-[:CATEGORYDETECT_IN_RPCATEGORY]-(cd:CategoryDetect)
                        WITH split(cd.Title," ") as splitCd, cd, rp2, detect3, arrayAsString, product
                        WITH splitCd, reduce(v='|', x in splitCd | v + x + '|') as testAsString, cd, rp2, product, detect3, arrayAsString
                        WHERE arrayAsString CONTAINS testAsString
                        WITH length(split(replace(arrayAsString,testAsString,"@"), "@")) -1 as dd, cd, rp2 , product, detect3, arrayAsString
                        //--------end match category by categorydetect-------
                        //--------start create category and spec rels-------
                        MERGE (product)-[:CATEGORYDETECT_HAS_PRODUCT {DetectCount:dd}]-(cd)
                        WITH sum(dd) as sumCd, rp2, product, detect3, arrayAsString
                        MERGE (product)-[:RP_CATEGORY_HAS_PRODUCT {DetectCount:sumCd}]-(rp2)
                        WITH sumCd, rp2, product, detect3, arrayAsString ORDER BY sumCd DESC limit 1
                        MERGE (rp2)-[:PRODUCT_HAS_MAIN_RPCATEGORY {DetectCount:sumCd}]-(product)
                        WITH detect3, product, rp2, arrayAsString
                        MATCH (cd3:SpecDetect)-[:SPECDETECT_IN_SPEC]-(:Spec)-[:SPEC_HAS_CATEGORY]-(rp2)
                        WHERE cd3.Id IN detect3
                        MERGE (product)-[r5:SPECDETECT_HAS_PRODUCT]->(cd3)
                        WITH distinct arrayAsString, product
                        //--------end create category and spec rels-------
                        //--------start match brand by branddetect-------
                        MATCH (s:SiteConfiguration)-[:BRAND_IN_SITE]-(c3:Brand)-[:BRANDDEDETECT_IN_BRAND]-(cd3:BrandDetect)
                        WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                        WITH split(cd3.Title," ") as d3, c3, cd3, arrayAsString, product
                        WITH d3,reduce(v='|', x in d3 | v + x + '|') as testAsString, cd3, c3, product, arrayAsString
                        WHERE arrayAsString CONTAINS testAsString
                        WITH length(split(replace(arrayAsString,testAsString,"@"), "@"))-1 as dd, cd3, c3, product, arrayAsString
                        MERGE (product)-[:BRANDDETECT_HAS_PRODUCT {DetectCount:dd}]-(cd3)
                        WITH sum(dd) as wordCount3, c3 as cat3, product, arrayAsString
                        MERGE (product)-[:PRODUCT_HAS_BRAND {DetectCount:wordCount3}]-(cat3)
                        WITH wordCount3, cat3, product, arrayAsString order by wordCount3 DESC limit 1
                        MERGE (cat3)-[:PRODUCT_HAS_MAIN_BRAND {DetectCount:wordCount3}]-(product)
                        WITH distinct arrayAsString, product
                        //--------end match brand by branddetect-------
                        //--------start match fact by factdetect-------
                        MATCH (s:SiteConfiguration)-[:FACT_IN_SITE]-(c3:Fact)-[:FACTDETECT_IN_FACT]-(cd3:FactDetect)
                        WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                        WITH split(cd3.Title," ") as d3, c3, cd3, arrayAsString, product
                        WITH d3,reduce(v='|', x in d3 | v + x + '|') as testAsString, cd3, c3, product, arrayAsString
                        WHERE arrayAsString CONTAINS testAsString
                        WITH length(split(replace(arrayAsString,testAsString,"@"), "@"))-1 as dd, cd3, c3, product, arrayAsString
                        MERGE (product)-[:FACTDETECT_HAS_PRODUCT {DetectCount:dd}]-(cd3)
                        WITH sum(dd) as wordCount3, c3 as cat3, product, arrayAsString
                        MERGE (product)-[:FACT_HAS_PRODUCT {DetectCount:wordCount3}]-(cat3)
                        //--------end match fact by factdetect-------""".trimMargin()

            db.execute(queryDelete)
            db.execute(query)
            return true
        } catch (e: Exception) {
            log.info("Error: " + e.message)
            return false
        }
    }

}

class EngineLable {
    companion object {
        fun productLabel(): Label = Label.label("Product")
        fun rsLabel(): Label = Label.label("RS")
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