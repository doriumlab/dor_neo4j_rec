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


    @UserFunction(name = "dor.counter")
    fun counter(@Name("Detect") pattern: String, @Name("Description") text: String): Number {
        var sTemp = text.toLowerCase()
        var counter = 0
        val sPattern = " "+ pattern + " "

        while (sTemp.length > 0) {
            val index = sTemp.indexOf(sPattern)
            if (index == -1) break
            sTemp = sTemp.substring(index + sPattern.length, sTemp.length)
            counter++
        }
        return counter
    }

    @Procedure(name = "dor.createProduct", mode = Mode.WRITE)
    fun createProduct(@Name("FaTitle") FaTitle: String, @Name("EnTitle") EnTitle: String, @Name("Description") Description: String, @Name("Price") Price: Long, @Name("SourceURL") SourceUrl: String, @Name("ImagePath") ImagePath: String) {
        val newquery = """
        WITH split("My phone frequently calls drop frequently with the iPhone"," ") as words
        UNWIND range(0,size(words)-2) as i
        merge(w3:Word {name:words[i],hash:dor.sha256(words[i]+i)})
        merge(w4:Word {name:words[i+1],hash:dor.sha256(words[i+1]+(i+1))})
        create (w3)-[:NEXT]->(w4)
         """

        val hashFaTitle = Sha256Maker(FaTitle)
        val product: Node? = db.findNode(EngineLable.productLabel(), "HashTitle", hashFaTitle)

        if (product == null){
            val id = CreateUUID()
            val q = """CREATE (c:Product { FaTitle:"$FaTitle", EnTitle:"$EnTitle", Description:"$Description", Price:"$Price", SourceUrl:"$SourceUrl", ImagePath:"$ImagePath", HashTitle:"$hashFaTitle",Id:"$id" })
                   WITH c
                   MATCH (rs:RS)
                   WHERE rs.SiteId = "50cfc9e8-402b-495b-8ed4-66dcb2b3aadd"
                   CREATE UNIQUE (rs)<-[:${Relations.PRODUCT_IN_RS.toString()}]-(c)""".trimMargin()

            val descQuery = """
                //Description Chain
                   MATCH (c:Product)
                   WHERE c.Id = "$id"
                   WITH reduce(t=trim(toLower(c.Description)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalized, c
                   WITH split(normalized," ") as words, c
                   UNWIND range(0,size(words)-2) as i
                   MERGE (w3:Word {Title:words[i],Hash:dor.sha256(words[i]+i+c.Id)})
                   MERGE (w4:Word {Title:words[i+1],Hash:dor.sha256(words[i+1]+(i+1)+c.Id)})
                   CREATE (w3)-[:NEXT]->(w4)
                   WITH c, words
                   MATCH (w:Word {Hash:dor.sha256(words[0]+0+c.Id)})
                   MERGE (c)-[:PRODUCT_HAS_DESCRIPTION]->(w)""".trimMargin()

            val titleQuery = """
                   //Persian Title Chain
                   MATCH (c:Product)
                   WHERE c.Id = "$id"
                   WITH reduce(t=trim(toLower(c.FaTitle)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalized, c
                   WITH split(normalized," ") as words, c
                   UNWIND range(0,size(words)-2) as i
                   MERGE(w3:Word {Title:words[i],Hash:dor.sha256(words[i]+i+c.Id)})
                   MERGE(w4:Word {Title:words[i+1],Hash:dor.sha256(words[i+1]+(i+1)+c.Id)})
                   CREATE (w3)-[:NEXT]->(w4)
                   WITH c, words
                   MATCH (w:Word {Hash:dor.sha256(words[0]+0+c.Id)})
                   MERGE (c)-[:PRODUCT_HAS_PERSIANTITLE]->(w)""".trimMargin()

            val enTitleQuery = """
                   //English Title Chain
                   MATCH (c:Product)
                   WHERE c.Id = "$id"
                   WITH reduce(t=trim(toLower(c.EnTitle)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalized, c
                   WITH split(normalized," ") as words, c
                   UNWIND range(0,size(words)-2) as i
                   MERGE(w3:Word {Title:words[i],Hash:dor.sha256(words[i]+i+c.Id)})
                   MERGE(w4:Word {Title:words[i+1],Hash:dor.sha256(words[i+1]+(i+1)+c.Id)})
                   CREATE (w3)-[:NEXT]->(w4)
                   WITH c, words
                   MATCH (w:Word {Hash:dor.sha256(words[0]+0+c.Id)})
                   MERGE (c)-[:PRODUCT_HAS_ENGLISHTITLE]->(w)
                   """.trimMargin()

            db.execute(q)
            db.execute(descQuery)
            db.execute(titleQuery)
            db.execute(enTitleQuery)


            val product: Node = db.findNode(EngineLable.productLabel(), "Id", id)
            analyseProduct(product)
        }

    }

    @Procedure(name = "dor.defineProduct", mode = Mode.WRITE)
    fun defineProduct(@Name("HashTitle") hashTitle: String) {
        val product: Node = db.findNode(EngineLable.productLabel(), "HashTitle", hashTitle)
        var s = analyseProduct(product)
        log.info("status: " + s.toString())
    }

    @Procedure(name = "dor.defineAllProducts", mode = Mode.WRITE)
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


    private fun analyseProduct(product: Node): Boolean {

        try {

//            deleteProductRelation(product)

//            var query = """MATCH (p:Product {HashTitle:'${product.getProperty("HashTitle")}'} )-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(:Word)-[:NEXT*0..]-(w:Word)
//                           WITH w, p
//                           MATCH (s:SiteConfiguration)-[:RP_CATEGORY_IN_SITE]-(c:RPCategory)-[:CATEGORYDETECT_IN_RPCATEGORY]-(cd:CategoryDetect)
//                           WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
//                           WITH split(cd.Title," ") as d, c, cd, collect(w.Title) as word, p
//                           WHERE ALL(x in d WHERE x in word)
//                          // WITH count(w) as wordCount,c as cat,collect(DISTINCT cd.Id) as detect, p
//                           WITH reduce(t=trim(toLower(p.Description)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalizedDesc, cd, c, p
//                           WITH reduce(t=trim(toLower(p.FaTitle)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalizedFaTitle, cd, c, p, normalizedDesc
//                           WITH reduce(t=trim(toLower(p.EnTitle)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalizedEnTitle, cd, c, p, normalizedDesc, normalizedFaTitle
//                           WITH dor.counter(cd.Title,toString(normalizedDesc + " " + normalizedFaTitle + " " + normalizedEnTitle)) as dd, cd, c, p
//                           WITH sum(dd) as wordCount, collect(cd.Id) as detect, c as cat, p
//                           MATCH (cd2:CategoryDetect)
//                           WHERE cd2.Id in detect
//                           WITH cd2, p, cat, wordCount
//                           OPTIONAL MATCH (p)-[r:CATEGORYDETECT_HAS_PRODUCT]-(:CategoryDetect)
//                           DELETE r
//                           WITH cd2, p, cat, wordCount
//                           OPTIONAL MATCH (p)-[r2:RP_CATEGORY_HAS_PRODUCT]-(:RPCategory)
//                           DELETE r2
//                           WITH cd2, p, cat, wordCount
//                           OPTIONAL MATCH (p)<-[r3:PRODUCT_HAS_MAIN_RPCATEGORY]-(:RPCategory)
//                           DELETE r3
//                           WITH cd2, p, cat, wordCount
//                           MERGE (p)-[:CATEGORYDETECT_HAS_PRODUCT]-(cd2)
//                           MERGE (p)-[:RP_CATEGORY_HAS_PRODUCT {DetectCount:wordCount}]-(cat)
//                           WITH wordCount, cat, p  order by wordCount DESC limit 1
//                           MERGE (cat)-[:PRODUCT_HAS_MAIN_RPCATEGORY {DetectCount:wordCount}]-(p)
//                           """.trimMargin()
            var query = """
                       MATCH (product:Product {HashTitle:'${product.getProperty("HashTitle")}'})-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(:Word)-[:NEXT*0..]-(w:Word)
                       WITH w, product
                       //----------------------------
                       MATCH (s:SiteConfiguration)-[:RP_CATEGORY_IN_SITE]-(rp:RPCategory)-[:SPEC_HAS_CATEGORY]-(spec:Spec)-[:SPECDETECT_IN_SPEC]-(spd:SpecDetect)
                       WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                       WITH split(spd.Title," ") as splitSpd, spec, spd, rp, collect(w.Title) as word, product
                       WHERE ALL(x in splitSpd WHERE x in word)
                       With rp, spec, spd, word, product
                       //-----------------------------
                       Match (rp)-[:RPC_IS_RPC_CHILD*0..]-(rp2:RPCategory)-[:CATEGORYDETECT_IN_RPCATEGORY]-(cd:CategoryDetect)
                       WITH split(cd.Title," ") as splitCd, cd, rp2, spec, spd, word, product
                       WHERE ALL (x in splitCd WHERE x in word)
                       WITH dor.counter(cd.Title,toString(product.Description + " " + product.FaTitle + " " + product.EnTitle)) as dd, cd, rp2 ,spec, product, spd , word
                       //-----------------------------
                       WITH sum(dd) as wordCount, collect(cd.Id) as detect, rp2 as cat, product, spec,spd
                       MATCH (cd2:CategoryDetect)
                       WHERE cd2.Id in detect
                       WITH cd2, product, cat, wordCount, spec,spd
                       //----------------------------
                       OPTIONAL MATCH (product)-[r:CATEGORYDETECT_HAS_PRODUCT]-(:CategoryDetect)
                       DELETE r
                       WITH cd2, product, spec,spd , cat, wordCount
                       OPTIONAL MATCH (product)-[r2:RP_CATEGORY_HAS_PRODUCT]-(:RPCategory)
                       DELETE r2
                       WITH cd2, product, spec,spd , cat, wordCount
                       OPTIONAL MATCH (product)<-[r3:PRODUCT_HAS_MAIN_RPCATEGORY]-(:RPCategory)
                       DELETE r3
                       WITH cd2, product, spec,spd , cat, wordCount
                       OPTIONAL MATCH (product)<-[r5:PRODUCT_HAS_SPECDETECT]-(:SpecDetect)
                       DELETE r5
                       //----------------------
                       WITH cd2, product, spec,spd , cat, wordCount
                       MERGE (product)-[:CATEGORYDETECT_HAS_PRODUCT]-(cd2)
                       MERGE (product)-[:RP_CATEGORY_HAS_PRODUCT {DetectCount:wordCount}]-(cat)
                       WITH collect(DISTINCT spd.Id) as detect, wordCount, cat, product
                       WITH wordCount, cat, product, detect ORDER BY wordCount DESC limit 1
                       MATCH (spd2:SpecDetect)-[:SPECDETECT_IN_SPEC]-(:Spec)-[:SPEC_HAS_CATEGORY]-(cat)
                       WHERE spd2.Id in detect
                       MERGE (spd2)<-[:SPECDETECT_HAS_PRODUCT]-(product)
                       MERGE (cat)-[:PRODUCT_HAS_MAIN_RPCATEGORY {DetectCount:wordCount}]-(product)""".trimMargin()

            var queryFact = """
                           MATCH (p:Product {HashTitle:'${product.getProperty("HashTitle")}'})-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(:Word)-[:NEXT*0..]-(w2:Word)
                           WITH w2, p
                           MATCH (p)-[:PRODUCT_HAS_MAIN_RPCATEGORY]-(rp:RPCategory)-[:RPC_IS_RPC_CHILD*0..]->(rp2:RPCategory)
                           WITH p,w2,rp2
                           MATCH (rp2)-[:SPEC_HAS_CATEGORY]-(:Spec)-[:FACTDETECT_IN_SPEC]-(cd2:FactDetect)-[:FACTDETECT_HAS_FACT]-(c2:Fact)
                           WITH split(cd2.Title," ") as d3, c2, cd2, collect(w2.Title) as word, p
                           WHERE ALL(x in d3 WHERE x in word)
                           WITH reduce(t=trim(toLower(p.Description)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalizedDesc, cd2, c2, p
                           WITH reduce(t=trim(toLower(p.FaTitle)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalizedFaTitle, cd2, c2, p, normalizedDesc
                           WITH reduce(t=trim(toLower(p.EnTitle)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalizedEnTitle, cd2, c2, p, normalizedDesc, normalizedFaTitle
                           WITH dor.counter(cd2.Title,toString(normalizedDesc + " " + normalizedFaTitle + " " + normalizedEnTitle)) as dd, cd2, c2, p
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
                           MATCH (p:Product {HashTitle:'${product.getProperty("HashTitle")}'})-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(:Word)-[:NEXT*0..]-(w3:Word)
                           WITH p, w3
                           MATCH (s3:SiteConfiguration)-[:BRAND_IN_SITE]-(c3:Brand)-[:BRANDDEDETECT_IN_BRAND]-(cd3:BrandDetect)
                           WHERE s3.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                           WITH split(cd3.Title," ") as d3, c3, cd3, collect(w3.Title) as word, p
                           WHERE ALL(x in d3 WHERE x in word)
                           WITH reduce(t=trim(toLower(p.Description)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalizedDesc, cd3, c3, p
                           WITH reduce(t=trim(toLower(p.FaTitle)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalizedFaTitle, cd3, c3, p, normalizedDesc
                           WITH reduce(t=trim(toLower(p.EnTitle)), delim in ["’", "'", "[", "]", "(", ")", "{", "}", "⟨", "⟩", ":", ",", "،", "、", "‒", "–", "—", "…", "...", "⋯", "᠁", "ฯ", "!", ".", "‹", "›", "«", "»", "‐", "-", "?", "‘", "’", "“", "”", "'", "'", '"', ";", "/", "·", "&", "*", "@", "\\", "•", " ^ ", "°", "”", "#", "÷", "×", "º", "ª", "%", "‰", "+", "−", "=", "‱", "¶", "′", "″", "‴", "§", "~", "_", "|", "‖", "¦", "©", "℗", "®", "℠", "،", "؟", "»", "«", "-", "؛", "..."] | replace(t,delim,"")) as normalizedEnTitle, cd3, c3, p, normalizedDesc, normalizedFaTitle
                           WITH dor.counter(cd3.Title,toString(normalizedDesc + " " + normalizedFaTitle + " " + normalizedEnTitle)) as dd, cd3, c3, p
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

//            var query = """MATCH (p:Product)-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(:Word)-[:NEXT*]-(w:Word)
//                           WITH w, p
//                           MATCH (s:SiteConfiguration)-[:RP_CATEGORY_IN_SITE]-(c:RPCategory)-[:SPEC_HAS_CATEGORY]-(cd:FactDetect)
//                           WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
//                           WITH split(cd.Title," ") as d, c, cd, collect(w.Title) as word, p
//                           WHERE ALL(x in d WHERE x in word)
//                          // WITH count(w) as wordCount,c as cat,collect(DISTINCT cd.Id) as detect, p
//                           WITH dor.counter(cd.Title,toString(p.Description + " " + p.FaTitle + " " + p.EnTitle)) as dd, cd, c, p
//                           WITH sum(dd) as wordCount, collect(cd.Id) as detect, c as cat, p
//                           MATCH (cd2:CategoryDetect)
//                           WHERE cd2.Id in detect
//                           WITH cd2, p, cat, wordCount
//                           OPTIONAL MATCH (p)-[r:CATEGORYDETECT_HAS_PRODUCT]-(:CategoryDetect)
//                           DELETE r
//                           WITH cd2, p, cat, wordCount
//                           OPTIONAL MATCH (p)-[r2:RP_CATEGORY_HAS_PRODUCT]-(:RPCategory)
//                           DELETE r2
//                           WITH cd2, p, cat, wordCount
//                           OPTIONAL MATCH (p)<-[r3:PRODUCT_HAS_MAIN_RPCATEGORY]-(:RPCategory)
//                           DELETE r3
//                           WITH cd2, p, cat, wordCount
//                           MERGE (p)-[:CATEGORYDETECT_HAS_PRODUCT]-(cd2)
//                           MERGE (p)-[:RP_CATEGORY_HAS_PRODUCT {DetectCount:wordCount}]-(cat)
//                           WITH wordCount, cat, p  order by wordCount DESC limit 1
//                           MERGE (cat)-[:PRODUCT_HAS_MAIN_RPCATEGORY {DetectCount:wordCount}]-(p)""".trimMargin()

            var query = """
                       MATCH (product:Product)-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(:Word)-[:NEXT*0..]-(w:Word)
                       WITH w, product
                       //----------------------------
                       MATCH (s:SiteConfiguration)-[:RP_CATEGORY_IN_SITE]-(rp:RPCategory)-[:SPEC_HAS_CATEGORY]-(spec:Spec)-[:SPECDETECT_IN_SPEC]-(spd:SpecDetect)
                       WHERE s.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                       WITH split(spd.Title," ") as splitSpd, spec, spd, rp, collect(w.Title) as word, product
                       WHERE ALL(x in splitSpd WHERE x in word)
                       With rp, spec, spd, word, product
                       //-----------------------------
                       Match (rp)-[:RPC_IS_RPC_CHILD*0..]-(rp2:RPCategory)-[:CATEGORYDETECT_IN_RPCATEGORY]-(cd:CategoryDetect)
                       WITH split(cd.Title," ") as splitCd, cd, rp2, spec, spd, word, product
                       WHERE ALL (x in splitCd WHERE x in word)
                       WITH dor.counter(cd.Title,toString(product.Description + " " + product.FaTitle + " " + product.EnTitle)) as dd, cd, rp2 ,spec, product, spd , word
                       //-----------------------------
                       WITH sum(dd) as wordCount, collect(cd.Id) as detect, rp2 as cat, product, spec,spd
                       MATCH (cd2:CategoryDetect)
                       WHERE cd2.Id in detect
                       WITH cd2, product, cat, wordCount, spec,spd
                       //----------------------------
                       OPTIONAL MATCH (product)-[r:CATEGORYDETECT_HAS_PRODUCT]-(:CategoryDetect)
                       DELETE r
                       WITH cd2, product, spec,spd , cat, wordCount
                       OPTIONAL MATCH (product)-[r2:RP_CATEGORY_HAS_PRODUCT]-(:RPCategory)
                       DELETE r2
                       WITH cd2, product, spec,spd , cat, wordCount
                       OPTIONAL MATCH (product)<-[r3:PRODUCT_HAS_MAIN_RPCATEGORY]-(:RPCategory)
                       DELETE r3
                       WITH cd2, product, spec,spd , cat, wordCount
                       OPTIONAL MATCH (product)<-[r5:PRODUCT_HAS_SPECDETECT]-(:SpecDetect)
                       DELETE r5
                       //----------------------
                       WITH cd2, product, spec,spd , cat, wordCount
                       MERGE (product)-[:CATEGORYDETECT_HAS_PRODUCT]-(cd2)
                       MERGE (product)-[:RP_CATEGORY_HAS_PRODUCT {DetectCount:wordCount}]-(cat)
                       WITH collect(DISTINCT spd.Id) as detect, wordCount, cat, product
                       WITH wordCount, cat, product, detect ORDER BY wordCount DESC limit 1
                       MATCH (spd2:SpecDetect)-[:SPECDETECT_IN_SPEC]-(:Spec)-[:SPEC_HAS_CATEGORY]-(cat)
                       WHERE spd2.Id in detect
                       MERGE (spd2)<-[:SPECDETECT_HAS_PRODUCT]-(product)
                       MERGE (cat)-[:PRODUCT_HAS_MAIN_RPCATEGORY {DetectCount:wordCount}]-(product)""".trimMargin()

            var queryFact = """
                           MATCH (p:Product)-[:PRODUCT_HAS_DESCRIPTION|:PRODUCT_HAS_PERSIANTITLE|:PRODUCT_HAS_ENGLISHTITLE]-(:Word)-[:NEXT*0..]-(w2:Word)
                           WITH w2, p
                           MATCH (s2:SiteConfiguration)-[:FACT_IN_SITE]-(c2:FACT)-[:FACTDETECT_IN_FACT]-(cd2:FactDetect)
                           WHERE s3.SiteId = 'a462b94d-687f-486b-9595-065922b09d8b'
                           WITH split(cd2.Title," ") as d3, c2, cd2, collect(w2.Title) as word, p
                           WHERE ALL(x in d3 WHERE x in word)
                           WITH dor.counter(cd2.Title,toString(p.Description + " " + p.FaTitle + " " + p.EnTitle)) as dd, cd2, c2, p
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
                           WITH dor.counter(cd3.Title,toString(p.Description + " " + p.FaTitle + " " + p.EnTitle)) as dd, cd3, c3, p
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