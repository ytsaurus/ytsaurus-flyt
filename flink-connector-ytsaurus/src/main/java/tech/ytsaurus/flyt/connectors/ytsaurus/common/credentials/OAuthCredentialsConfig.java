package tech.ytsaurus.flyt.connectors.ytsaurus.common.credentials;

import java.io.Serializable;

import lombok.Value;

@Value
public class OAuthCredentialsConfig implements Serializable {
    String username;
    String token;
}
