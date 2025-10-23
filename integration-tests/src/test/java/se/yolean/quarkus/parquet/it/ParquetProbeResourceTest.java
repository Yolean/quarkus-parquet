package se.yolean.quarkus.parquet.it;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class ParquetProbeResourceTest {

    @Test
    void parquetRoundTripShouldReturnExpectedRows() {
        given()
                .when().get("/parquet/probe")
                .then()
                .statusCode(200)
                .body(is("Alice:34,Bob:28"));
    }
}
