package com.moveinsync.scheduler;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Authentication, role-based access and tenant isolation, end to end through the HTTP layer. */
@SpringBootTest
@AutoConfigureMockMvc
@org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability
class ApiSecurityTest extends PgTestBase {

    @DynamicPropertySource
    static void db(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", PG::getJdbcUrl);
        r.add("spring.datasource.username", PG::getUsername);
        r.add("spring.datasource.password", PG::getPassword);
        r.add("scheduler.bench-enabled", () -> "true");
    }

    @Autowired MockMvc mvc;
    @Autowired PasswordEncoder encoder;

    @BeforeEach
    void users() {
        String hash = encoder.encode("pw");
        jdbc.update("INSERT INTO app_user VALUES ('ops', ?, 'OPERATOR', NULL), ('acme-admin', ?, 'ADMIN', 'acme'), "
                + "('acme-emp', ?, 'EMPLOYEE', 'acme'), ('globex-admin', ?, 'ADMIN', 'globex')", hash, hash, hash, hash);
    }

    private String login(String user) throws Exception {
        MvcResult r = mvc.perform(post("/auth/token").contentType(MediaType.APPLICATION_JSON)
                .content("{\"username\":\"" + user + "\",\"password\":\"pw\"}")).andExpect(status().isOk()).andReturn();
        return "Bearer " + mapper.readTree(r.getResponse().getContentAsString()).get("token").asText();
    }

    private String createFutureJob(String bearer) throws Exception {
        String body = "{\"name\":\"alert\",\"type\":\"PICKUP_ALERT\",\"runAt\":\"" + Instant.now().plus(Duration.ofDays(1)) + "\"}";
        MvcResult r = mvc.perform(post("/api/jobs").header("Authorization", bearer).contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isCreated()).andReturn();
        return mapper.readTree(r.getResponse().getContentAsString()).get("id").asText();
    }

    @Test
    void wrongPasswordAndUnknownUserGiveTheSameGenericAnswer() throws Exception {
        String a = mvc.perform(post("/auth/token").contentType(MediaType.APPLICATION_JSON).content("{\"username\":\"acme-admin\",\"password\":\"nope\"}"))
                .andExpect(status().isUnauthorized()).andReturn().getResponse().getContentAsString();
        String b = mvc.perform(post("/auth/token").contentType(MediaType.APPLICATION_JSON).content("{\"username\":\"someone@example.com\",\"password\":\"nope\"}"))
                .andExpect(status().isUnauthorized()).andReturn().getResponse().getContentAsString();
        assertThat(mapper.readTree(a).get("message")).isEqualTo(mapper.readTree(b).get("message"));
        assertThat(b).doesNotContain("someone@example.com");     // user-supplied identifiers are never echoed
    }

    @Test
    void missingOrForgedTokenIsUnauthorised() throws Exception {
        mvc.perform(get("/api/jobs")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/jobs").header("Authorization", "Bearer not.a.jwt")).andExpect(status().isUnauthorized());
    }

    @Test
    void tenantIsolation_oneClientCanNeverReadAnothersJobs() throws Exception {
        String acme = login("acme-admin"), globex = login("globex-admin");
        String jobId = createFutureJob(acme);

        mvc.perform(get("/api/jobs/" + jobId).header("Authorization", acme)).andExpect(status().isOk());
        // Same id, other tenant: indistinguishable from "does not exist".
        mvc.perform(get("/api/jobs/" + jobId).header("Authorization", globex)).andExpect(status().isNotFound());
        mvc.perform(get("/api/jobs/" + jobId + "/next-fires").header("Authorization", globex)).andExpect(status().isNotFound());
        mvc.perform(get("/api/jobs/" + jobId + "/runs").header("Authorization", globex)).andExpect(status().isNotFound());
        mvc.perform(post("/api/jobs/" + jobId + "/trigger").header("Authorization", globex)).andExpect(status().isNotFound());
        mvc.perform(delete("/api/jobs/" + jobId).header("Authorization", globex)).andExpect(status().isNotFound());
        // Naming the other tenant explicitly does not help either.
        mvc.perform(get("/api/jobs/" + jobId).param("tenant", "acme").header("Authorization", globex)).andExpect(status().isNotFound());
        mvc.perform(get("/api/jobs").param("tenant", "acme").header("Authorization", globex)).andExpect(status().isNotFound());
        mvc.perform(post("/api/tenants/acme/pause").header("Authorization", globex)).andExpect(status().isNotFound());
        mvc.perform(get("/api/dead-letters").param("tenant", "acme").header("Authorization", globex)).andExpect(status().isNotFound());

        JsonNode list = mapper.readTree(mvc.perform(get("/api/jobs").header("Authorization", globex)).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString());
        assertThat(list).isEmpty();
        assertThat(jdbc.queryForObject("SELECT paused FROM tenant WHERE id = 'acme'", Boolean.class)).isFalse();
        // The job still exists, untouched.
        mvc.perform(get("/api/jobs/" + jobId).header("Authorization", acme)).andExpect(status().isOk());
    }

    @Test
    void employeeIsReadOnly_adminCanWrite_operatorMustNameATenant() throws Exception {
        String emp = login("acme-emp"), admin = login("acme-admin"), ops = login("ops");
        String jobId = createFutureJob(admin);

        mvc.perform(get("/api/jobs/" + jobId).header("Authorization", emp)).andExpect(status().isOk());
        mvc.perform(get("/api/jobs/" + jobId + "/next-fires").header("Authorization", emp)).andExpect(status().isOk());
        mvc.perform(post("/api/jobs").header("Authorization", emp).contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"x\",\"type\":\"PICKUP_ALERT\"}")).andExpect(status().isForbidden());
        mvc.perform(post("/api/jobs/" + jobId + "/trigger").header("Authorization", emp)).andExpect(status().isForbidden());
        mvc.perform(post("/api/tenants/acme/pause").header("Authorization", emp)).andExpect(status().isForbidden());
        mvc.perform(get("/api/dead-letters").header("Authorization", emp)).andExpect(status().isForbidden());

        mvc.perform(get("/api/ops/summary").header("Authorization", admin)).andExpect(status().isForbidden());
        mvc.perform(get("/api/ops/summary").header("Authorization", ops)).andExpect(status().isOk());
        mvc.perform(get("/api/jobs").header("Authorization", ops)).andExpect(status().isBadRequest());
        mvc.perform(get("/api/jobs").param("tenant", "acme").header("Authorization", ops)).andExpect(status().isOk());
        mvc.perform(post("/api/tenants").header("Authorization", admin).contentType(MediaType.APPLICATION_JSON)
                .content("{\"id\":\"newco\"}")).andExpect(status().isForbidden());
    }

    @Test
    void metricsEndpointIsOperatorOnlyAndExposesTheOnCallCounters() throws Exception {
        mvc.perform(get("/actuator/prometheus")).andExpect(status().isUnauthorized());
        mvc.perform(get("/actuator/prometheus").header("Authorization", login("acme-admin"))).andExpect(status().isForbidden());
        String body = mvc.perform(get("/actuator/prometheus").header("Authorization", login("ops"))).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertThat(body).contains("scheduler_fire_lag_seconds", "scheduler_runs_running", "scheduler_deadletter_size",
                "scheduler_fencing_rejections_total", "scheduler_retrying_size", "scheduler_queue_oldest_due_ms",
                "scheduler_leases_takeovers_total", "scheduler_partitions_owned");
        mvc.perform(get("/actuator/health")).andExpect(status().isOk());
    }

    @Test
    void errorsAreMeaningfulAndLeakNothing() throws Exception {
        String admin = login("acme-admin");
        String bad = mvc.perform(post("/api/jobs").header("Authorization", admin).contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"x\",\"type\":\"PICKUP_ALERT\",\"cron\":\"not a cron\"}")).andExpect(status().isBadRequest())
                .andReturn().getResponse().getContentAsString();
        assertThat(mapper.readTree(bad).get("code").asText()).isEqualTo("INVALID_CRON");
        assertThat(bad).doesNotContain("Exception", "at com.", "org.springframework", "SELECT");

        String malformed = mvc.perform(post("/api/jobs").header("Authorization", admin).contentType(MediaType.APPLICATION_JSON)
                .content("{ this is not json")).andExpect(status().isBadRequest()).andReturn().getResponse().getContentAsString();
        assertThat(malformed).doesNotContain("Unexpected character", "JsonParseException", "this is not json");

        mvc.perform(post("/api/jobs").header("Authorization", admin).contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"x\",\"type\":\"NOPE\"}")).andExpect(status().isBadRequest());
        mvc.perform(post("/api/jobs").header("Authorization", admin).contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"x\",\"type\":\"PICKUP_ALERT\",\"timeZone\":\"Mars/Base\"}")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/jobs/not-a-uuid").header("Authorization", admin)).andExpect(status().isBadRequest());
    }

    @Test
    void deletingAJobCancelsItsQueuedRuns() throws Exception {
        String admin = login("acme-admin");
        String body = "{\"name\":\"alert\",\"type\":\"PICKUP_ALERT\",\"runAt\":\"" + Instant.now().plusSeconds(4) + "\"}";
        String id = mapper.readTree(mvc.perform(post("/api/jobs").header("Authorization", admin).contentType(MediaType.APPLICATION_JSON)
                .content(body)).andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asText();

        mvc.perform(delete("/api/jobs/" + id).header("Authorization", admin)).andExpect(status().isNoContent());

        assertThat(count("SELECT count(*) FROM job_run WHERE job_id = ? AND status = 'SKIPPED'", java.util.UUID.fromString(id))).isEqualTo(1);
        // well past the original due time nothing has been sent
        await().during(Duration.ofSeconds(6)).atMost(Duration.ofSeconds(10)).until(() -> count("SELECT count(*) FROM effect WHERE tenant_id = 'acme'") == 0);
    }

    @Test
    void triggerReturnsTheRunItCreated_notALaterCronOccurrence() throws Exception {
        String admin = login("acme-admin");
        String id = mapper.readTree(mvc.perform(post("/api/jobs").header("Authorization", admin).contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"frequent\",\"type\":\"PICKUP_ALERT\",\"cron\":\"*/10 * * * * *\",\"timeZone\":\"UTC\"}"))
                .andExpect(status().isCreated()).andReturn().getResponse().getContentAsString()).get("id").asText();
        java.util.UUID jobId = java.util.UUID.fromString(id);
        // the materialiser creates occurrences up to 30 s ahead; wait until one is in the future
        await().atMost(Duration.ofSeconds(10)).until(() -> count("SELECT count(*) FROM job_run WHERE job_id = ? AND scheduled_at > clock_timestamp()", jobId) >= 1);

        JsonNode run = mapper.readTree(mvc.perform(post("/api/jobs/" + id + "/trigger").header("Authorization", admin))
                .andExpect(status().isAccepted()).andReturn().getResponse().getContentAsString());

        long returnedId = run.get("id").asLong();
        assertThat(count("SELECT count(*) FROM job_run WHERE job_id = ? AND scheduled_at > (SELECT scheduled_at FROM job_run WHERE id = ?)", jobId, returnedId))
                .as("later occurrences exist, so the newest run by schedule is not the triggered one").isPositive();
        assertThat(run.get("runAt").asText()).isEqualTo(run.get("scheduledAt").asText());   // a manual run is due immediately
    }

    @Test
    void tenantCreationIsCreateOnlyAndValidated() throws Exception {
        String ops = login("ops");
        mvc.perform(post("/api/tenants").header("Authorization", ops).contentType(MediaType.APPLICATION_JSON)
                .content("{\"id\":\"newco\",\"timeZone\":\"Asia/Tokyo\",\"maxConcurrency\":5}")).andExpect(status().isCreated());
        jdbc.update("UPDATE tenant SET max_concurrency = 9 WHERE id = 'newco'");

        mvc.perform(post("/api/tenants").header("Authorization", ops).contentType(MediaType.APPLICATION_JSON)
                .content("{\"id\":\"newco\"}")).andExpect(status().isConflict());
        assertThat(count("SELECT max_concurrency FROM tenant WHERE id = 'newco'")).as("a repeated POST must not reset the cap").isEqualTo(9);
        assertThat(jdbc.queryForObject("SELECT time_zone FROM tenant WHERE id = 'newco'", String.class)).isEqualTo("Asia/Tokyo");

        mvc.perform(post("/api/tenants").header("Authorization", ops).contentType(MediaType.APPLICATION_JSON)
                .content("{\"id\":\"badzone\",\"timeZone\":\"Mars/Base\"}")).andExpect(status().isBadRequest());
        mvc.perform(post("/api/tenants").header("Authorization", ops).contentType(MediaType.APPLICATION_JSON)
                .content("{\"id\":\"zerocap\",\"maxConcurrency\":0}")).andExpect(status().isBadRequest());
    }

    @Test
    void frameworkLevelErrorsKeepTheirStatusAndOurShape() throws Exception {
        String admin = login("acme-admin");
        String id = createFutureJob(admin);
        assertThat(mapper.readTree(mvc.perform(get("/api/does-not-exist").header("Authorization", admin)).andExpect(status().isNotFound())
                .andReturn().getResponse().getContentAsString()).get("code").asText()).isEqualTo("NOT_FOUND");
        assertThat(mapper.readTree(mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put("/api/jobs/" + id)
                .header("Authorization", admin)).andExpect(status().isMethodNotAllowed()).andReturn().getResponse().getContentAsString())
                .get("code").asText()).isEqualTo("METHOD_NOT_ALLOWED");
        mvc.perform(post("/api/jobs").header("Authorization", admin).contentType(MediaType.TEXT_PLAIN).content("x"))
                .andExpect(status().isUnsupportedMediaType());
    }

    @Test
    void aOneOffThatAlreadyFiredHasNoNextFire() throws Exception {
        String admin = login("acme-admin");
        String id = mapper.readTree(mvc.perform(post("/api/jobs").header("Authorization", admin).contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"now\",\"type\":\"PICKUP_ALERT\"}")).andExpect(status().isCreated()).andReturn().getResponse().getContentAsString())
                .get("id").asText();
        await().atMost(Duration.ofSeconds(20)).until(() -> count("SELECT count(*) FROM effect WHERE tenant_id = 'acme'") == 1);
        JsonNode fires = mapper.readTree(mvc.perform(get("/api/jobs/" + id + "/next-fires").header("Authorization", admin))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(fires.get("nextFires")).isEmpty();
    }

    @Test
    void nextFiresAreListedInTheTenantsTimeZone_andManualTriggerFiresExactlyOnce() throws Exception {
        String admin = login("acme-admin");
        MvcResult r = mvc.perform(post("/api/jobs").header("Authorization", admin).contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"roster\",\"type\":\"ROSTER_GENERATION\",\"cron\":\"0 0 22 * * *\",\"timeZone\":\"America/New_York\"}"))
                .andExpect(status().isCreated()).andReturn();
        String id = mapper.readTree(r.getResponse().getContentAsString()).get("id").asText();

        JsonNode fires = mapper.readTree(mvc.perform(get("/api/jobs/" + id + "/next-fires").param("count", "3").header("Authorization", admin))
                .andExpect(status().isOk()).andReturn().getResponse().getContentAsString());
        assertThat(fires.get("nextFires")).hasSize(3);
        assertThat(fires.get("timeZone").asText()).isEqualTo("America/New_York");

        mvc.perform(post("/api/jobs/" + id + "/trigger").header("Authorization", admin)).andExpect(status().isAccepted());
        await().atMost(Duration.ofSeconds(20)).until(() -> count("SELECT count(*) FROM effect WHERE tenant_id = 'acme' AND kind = 'ROSTER'") == 1);
    }
}
