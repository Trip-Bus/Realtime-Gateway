package com.tripbus.realtime.push;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Servicio de push simulado (como FCM o Mozilla autopush): guarda cada petición y responde 201, salvo las rutas
 * que empiezan por /gone (410), /missing (404) o /error (500). También genera llaves P-256 como las de un navegador o VAPID.
 */
public final class FakePushServer implements AutoCloseable {

    /** Una petición recibida. */
    public record Received(String path, Map<String, List<String>> headers, int bodyLength) {
        public String header(String name) {
            return headers.entrySet().stream().filter(e -> e.getKey().equalsIgnoreCase(name))
                    .map(e -> e.getValue().get(0)).findFirst().orElse(null);
        }
    }

    public final List<Received> received = new CopyOnWriteArrayList<>();
    private final HttpServer server;

    public FakePushServer() {
        try {
            server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        server.createContext("/", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String path = exchange.getRequestURI().getPath();
            received.add(new Received(path, Map.copyOf(exchange.getRequestHeaders()), body.length));
            int status = path.startsWith("/gone") ? 410 : statusFor(path);
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        });
        server.start();
    }

    private static int statusFor(String path) {
        if (path.startsWith("/missing")) {
            return 404;
        }
        return path.startsWith("/error") ? 500 : 201;
    }

    public String url(String path) {
        return "http://localhost:" + server.getAddress().getPort() + path;
    }

    public List<Received> to(String path) {
        return received.stream().filter(r -> r.path().equals(path)).toList();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    /** Par de llaves P-256 en el formato de Web Push: pública sin comprimir y privada (d), en base64url. */
    public record Keys(String publicKey, String privateKey) {
    }

    public static Keys newKeys() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("EC");
            generator.initialize(new ECGenParameterSpec("secp256r1"));
            KeyPair pair = generator.generateKeyPair();
            ECPublicKey pub = (ECPublicKey) pair.getPublic();
            byte[] point = new byte[65];
            point[0] = 0x04;
            System.arraycopy(fixed(pub.getW().getAffineX()), 0, point, 1, 32);
            System.arraycopy(fixed(pub.getW().getAffineY()), 0, point, 33, 32);
            byte[] d = fixed(((ECPrivateKey) pair.getPrivate()).getS());
            Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
            return new Keys(encoder.encodeToString(point), encoder.encodeToString(d));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /** 16 bytes al azar en base64url: el secreto auth de una suscripción. */
    public static String newAuth() {
        byte[] auth = new byte[16];
        new SecureRandom().nextBytes(auth);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(auth);
    }

    private static byte[] fixed(BigInteger value) {
        byte[] raw = value.toByteArray();
        byte[] out = new byte[32];
        int copy = Math.min(raw.length, 32);
        System.arraycopy(raw, raw.length - copy, out, 32 - copy, copy);
        return out;
    }
}
