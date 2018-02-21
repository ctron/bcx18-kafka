/*******************************************************************************
 * Copyright (c) 2017 Red Hat Inc and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     Jens Reimann - initial API and implementation
 *******************************************************************************/
package de.dentrassi.bcx18.kafka;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.eclipse.iot.unide.ppmp.measurements.MeasurementsWrapper;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class RouteBuilder extends org.apache.camel.builder.RouteBuilder {

    @Bean(name = "mydatasource")
    public DataSource dataSource() {
        final BasicDataSource result = new BasicDataSource();
        result.setDriverClassName("org.mariadb.jdbc.Driver");
        result.setUrl("jdbc:mariadb://100.102.4.11:3306/bcx18");
        result.setUsername("bcx18");
        result.setPassword("bcx18");
        return result;
    }

    @Override
    public void configure() throws Exception {
        from("kafka:Packaging_Simulator?brokers=100.102.4.11:9092")
                .convertBodyTo(MeasurementsWrapper.class)
                .to("stream:out")
                .bean(Processor.class, "insert")
                .to("stream:out")
                .to("jdbc:mydatasource");
    }

}
