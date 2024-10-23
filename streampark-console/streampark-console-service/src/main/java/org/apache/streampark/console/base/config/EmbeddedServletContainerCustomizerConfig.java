package org.apache.streampark.console.base.config;

import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.SecurityConstraint;
import io.undertow.servlet.api.WebResourceCollection;
import org.springframework.boot.web.embedded.undertow.UndertowDeploymentInfoCustomizer;
import org.springframework.boot.web.embedded.undertow.UndertowServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;

@Configuration
public class EmbeddedServletContainerCustomizerConfig {

  @Bean
  public WebServerFactoryCustomizer containerCustomizer() {
    return new WebServerFactoryCustomizer() {
      @Override
      public void customize(WebServerFactory factory) {
        if (factory.getClass().isAssignableFrom(UndertowServletWebServerFactory.class)) {
          UndertowServletWebServerFactory underTowContainer =
              (UndertowServletWebServerFactory) factory;
          underTowContainer.addDeploymentInfoCustomizers(new ContextSecurityCustomizer());
        }
      }
    };
  }

  private static class ContextSecurityCustomizer implements UndertowDeploymentInfoCustomizer {
    @Override
    public void customize(DeploymentInfo deploymentInfo) {
      SecurityConstraint constraint = new SecurityConstraint();
      WebResourceCollection traceWebresource = new WebResourceCollection();
      traceWebresource.addUrlPattern("/*");
      traceWebresource.addHttpMethod(HttpMethod.TRACE.toString());
      constraint.addWebResourceCollection(traceWebresource);
      deploymentInfo.addSecurityConstraint(constraint);
    }
  }
}
