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

package org.killbill.billing.plugin.meter;

import java.util.Hashtable;
import java.util.Properties;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;

import org.killbill.billing.invoice.plugin.api.InvoiceFormatterFactory;
import org.killbill.billing.osgi.api.OSGIPluginProperties;
import org.killbill.billing.osgi.libs.killbill.KillbillActivatorBase;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillEventDispatcher;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillEventDispatcher.OSGIFrameworkEventHandler;
import org.killbill.billing.plugin.api.notification.PluginConfigurationEventHandler;
import org.killbill.billing.plugin.core.config.PluginEnvironmentConfig;
import org.killbill.billing.plugin.core.resources.jooby.PluginApp;
import org.killbill.billing.plugin.core.resources.jooby.PluginAppBuilder;
import org.killbill.billing.plugin.meter.contoller.MeterController;
import org.killbill.billing.plugin.meter.dao.RawUsageDao;
import org.killbill.billing.plugin.meter.service.MeterService;
import org.osgi.framework.BundleContext;
import org.osgi.util.tracker.ServiceTracker;

/**
 * Hello world!
 *
 */
public class MeterRegister extends KillbillActivatorBase
{
    public static final String PLUGIN_NAME = "meter-plugin";

    private MeterConfigurationHandler meterConfigurationHandler;
    private OSGIKillbillEventDispatcher.OSGIKillbillEventHandler killbillEventHandler;

    private ServiceTracker<InvoiceFormatterFactory, InvoiceFormatterFactory> invoiceFormatterTracker;

    private MeterService meterService;


    @Override
    public void start(final BundleContext context) throws Exception {
        super.start(context);

        final String region = PluginEnvironmentConfig.getRegion(configProperties.getProperties());

        // Register an event listener for plugin configuration (optional)
        meterConfigurationHandler = new MeterConfigurationHandler(region, PLUGIN_NAME, killbillAPI);
        final Properties globalConfiguration = meterConfigurationHandler
                .createConfigurable(configProperties.getProperties());
        meterConfigurationHandler.setDefaultConfigurable(globalConfiguration);

        // create a service tracker for a custom InvoiceFormatter service
        invoiceFormatterTracker = new ServiceTracker<>(context, InvoiceFormatterFactory.class, null);
        invoiceFormatterTracker.open();

        final RawUsageDao rawUsageDao=new RawUsageDao(dataSource.getDataSource());

        meterService = new MeterService(rawUsageDao, killbillAPI);


        // Register a servlet (optional)
        final PluginApp pluginApp = new PluginAppBuilder(PLUGIN_NAME, killbillAPI, dataSource, super.clock,
                                                         configProperties)
                .withRouteClass(MeterController.class)
                .withService(meterService)
                .build();
        final HttpServlet httpServlet = PluginApp.createServlet(pluginApp);
        registerServlet(context, httpServlet);

        registerHandlers();
    }

    @Override
    public void stop(final BundleContext context) throws Exception {
        super.stop(context);
    }

    private void registerHandlers() {
        final PluginConfigurationEventHandler configHandler = new PluginConfigurationEventHandler(
                meterConfigurationHandler);

        dispatcher.registerEventHandlers(configHandler,
                                         (OSGIFrameworkEventHandler) () -> dispatcher.registerEventHandlers(killbillEventHandler));
    }

    private void registerServlet(final BundleContext context, final Servlet servlet) {
        final Hashtable<String, String> props = new Hashtable<String, String>();
        props.put(OSGIPluginProperties.PLUGIN_NAME_PROP, PLUGIN_NAME);
        registrar.registerService(context, Servlet.class, servlet, props);
    }
}
