package com.codingful.tandem.admin.relay;

import com.codingful.tandem.admin.relay.dto.BucketSelectorRequest;
import com.codingful.tandem.admin.relay.dto.BucketStatusResponse;
import com.codingful.tandem.admin.relay.dto.RelayStatusResponse;
import com.codingful.tandem.admin.relay.dto.WorkerInfoResponse;
import java.security.Principal;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST driving adapter for the relay-control endpoints (slice 3, HLD-admin-api §2/§4.1), realising
 * {@code admin-api.openapi.yaml}'s {@code Relay} tag. Delegates every use case to
 * {@link RelayAdminService}; this class only binds HTTP onto it.
 */
@RestController
@RequestMapping("${tandem.admin.base-path:/tandem/admin}/v1/relay")
class RelayAdminController {

    private final RelayAdminService service;

    RelayAdminController(RelayAdminService service) {
        this.service = service;
    }

    @GetMapping("/status")
    RelayStatusResponse status() {
        return service.status();
    }

    @PostMapping("/pause")
    RelayStatusResponse pause(@RequestBody(required = false) BucketSelectorRequest request, Principal principal) {
        return service.pause(bucketOf(request), actorOf(principal));
    }

    @PostMapping("/resume")
    RelayStatusResponse resume(@RequestBody(required = false) BucketSelectorRequest request, Principal principal) {
        return service.resume(bucketOf(request), actorOf(principal));
    }

    @GetMapping("/buckets")
    List<BucketStatusResponse> buckets(@RequestParam(name = "uncoveredOnly", defaultValue = "false") boolean uncoveredOnly) {
        return service.buckets(uncoveredOnly);
    }

    @GetMapping("/buckets/{bucket}")
    BucketStatusResponse bucket(@PathVariable("bucket") int bucket) {
        return service.bucket(bucket);
    }

    @PostMapping("/buckets/{bucket}/release")
    BucketStatusResponse release(@PathVariable("bucket") int bucket, Principal principal) {
        return service.releaseBucket(bucket, actorOf(principal));
    }

    @GetMapping("/workers")
    List<WorkerInfoResponse> workers() {
        return service.workers();
    }

    private static Integer bucketOf(BucketSelectorRequest request) {
        return request == null ? null : request.bucket();
    }

    /**
     * The caller's identity when the host application authenticates requests — see
     * {@code OutboxAdminController}'s javadoc for the full rationale; same mechanism, same fallback.
     */
    private static String actorOf(Principal principal) {
        return principal == null ? null : principal.getName();
    }
}
