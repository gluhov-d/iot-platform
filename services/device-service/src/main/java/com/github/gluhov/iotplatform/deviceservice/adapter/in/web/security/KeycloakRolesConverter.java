package com.github.gluhov.iotplatform.deviceservice.adapter.in.web.security;

import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Роли из {@code resource_access.<клиент>.roles} - роли клиента device-service (devices.read, devices.write) без префикса:
 * проверки в SecurityConfig - hasAuthority("devices.read").
 */
public class KeycloakRolesConverter implements Converter<Jwt, Collection<GrantedAuthority>> {

    private final String clientId;

    public KeycloakRolesConverter(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public Collection<GrantedAuthority> convert(Jwt jwt) {
        Map<String, Object> resourceAccess = jwt.getClaimAsMap("resource_access");
        if (resourceAccess == null || !(resourceAccess.get(clientId) instanceof Map<?, ?> client)
                || !(client.get("roles") instanceof Collection<?> roles)) {
            return List.of();
        }
        List<GrantedAuthority> authorities = new ArrayList<>();
        for (Object role : roles) {
            authorities.add(new SimpleGrantedAuthority(String.valueOf(role)));
        }
        return authorities;
    }
}
