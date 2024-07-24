/*
 * Copyright 2020-2024 Equinix, Inc
 * Copyright 2014-2024 The Billing Project, LLC
 *
 * The Billing Project licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.killbill.billing.plugin.meter.contoller;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.jooby.mvc.Body;
import org.jooby.mvc.GET;
import org.jooby.mvc.POST;
import org.jooby.mvc.Path;
import org.killbill.billing.plugin.meter.domain.external.ChargeDetails;
import org.killbill.billing.plugin.meter.domain.external.ConsumerSubscriptionUsageRecord;
import org.killbill.billing.plugin.meter.exception.RequestTooEarly;
import org.killbill.billing.plugin.meter.exception.ResourceNotFoundException;
import org.killbill.billing.plugin.meter.service.MeterService;
import lombok.extern.slf4j.Slf4j;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Singleton
@Path("/")
@Slf4j
public class MeterController {

    private final MeterService meterService;

    @Inject
    public MeterController(final MeterService meterService) {
        log.info("MeterController :: {}",meterService);
        this.meterService = meterService;
    }

    @POST
    public void addRawUsage(@Body final ConsumerSubscriptionUsageRecord request){
        meterService.addRawUsage(request);
    }

    @POST
    @Path("/aggregate")
    public void aggregateUsages(){
        meterService.aggregateUsages();
    }

    @POST
    @Path("/bill")
    public void bill(){
        meterService.bill();
    }

    @POST
    @Path("/invoice")
    public void invoice(){
        meterService.invoice();
    }

    @POST
    @Path("/back-fill")
    public void backFill(){
        meterService.backFill();
    }

    @POST
    @Path("/charge-usage")
    public void chargeUsage(){
        meterService.chargeUsage();
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("tenant/{tenantId}/subscription/{subscriptionId}/unit/{unit}/trackingId/{trackingId}")
    public ChargeDetails getCharges(
            @PathParam("subscriptionId") final String subscriptionId,
            @PathParam("tenantId") final String tenantId,
            @PathParam("unit") final String unit,
            @PathParam("trackingId") final String trackingId) {
        try{
            return meterService.getCharges(tenantId, subscriptionId, unit, trackingId);
        }catch (final Exception e){
            return new ChargeDetails().setReason(e.getMessage());
        }
    }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("tenant/{tenantId}/pg/subscription/{subscriptionId}/unit/{unit}/trackingId/{trackingId}")
    public ChargeDetails getChargesForPG(
            @PathParam("subscriptionId") final String subscriptionId,
            @PathParam("tenantId") final String tenantId,
            @PathParam("unit") final String unit,
            @PathParam("trackingId") final String trackingId) {
        try{
            return meterService.getChargesForPG(tenantId, subscriptionId, unit, trackingId);
        }catch (final Exception e){
            return new ChargeDetails().setReason(e.getMessage());
        }
    }
}
