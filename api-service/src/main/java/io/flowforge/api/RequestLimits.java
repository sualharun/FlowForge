package io.flowforge.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Bounds the request body before it is parsed.
 *
 * <p>Bean Validation rejects an oversized workflow only after Jackson has materialised the entire
 * object graph, so the 1000-task limit does not bound memory on its own, and Jackson's
 * {@code maxDocumentLength} constraint is not enforced by the bundled parser. Tomcat's own post-size
 * limit only covers form encoding, not JSON. This filter is therefore the only body cap the API has
 * when reached directly; the bundled reverse proxy applies the same 5 MiB to browser traffic.
 *
 * <p>A declared {@code Content-Length} over the cap is refused with 413 before a byte is read. A
 * chunked body without a declared length is bounded by counting as it is consumed, which surfaces
 * as a 400 from the message converter.
 */
@Component
public class RequestLimits extends OncePerRequestFilter {
    public static final int MAX_REQUEST_BYTES = 5 * 1024 * 1024;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws IOException, jakarta.servlet.ServletException {
        if (request.getContentLengthLong() > MAX_REQUEST_BYTES) {
            response.sendError(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE,
                    "Request body exceeds " + MAX_REQUEST_BYTES + " bytes");
            return;
        }
        chain.doFilter(new BoundedRequest(request), response);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !request.getRequestURI().startsWith("/api/");
    }

    /** Wraps the body so an undeclared (chunked) oversize is still stopped mid-read. */
    private static final class BoundedRequest extends HttpServletRequestWrapper {
        BoundedRequest(HttpServletRequest request) { super(request); }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            ServletInputStream delegate = super.getInputStream();
            return new ServletInputStream() {
                private long read;

                private int count(int value) throws IOException {
                    if (value != -1 && ++read > MAX_REQUEST_BYTES)
                        throw new IOException("Request body exceeds " + MAX_REQUEST_BYTES + " bytes");
                    return value;
                }

                @Override public int read() throws IOException { return count(delegate.read()); }

                @Override
                public int read(byte[] buffer, int offset, int length) throws IOException {
                    int n = delegate.read(buffer, offset, length);
                    if (n > 0 && (read += n) > MAX_REQUEST_BYTES)
                        throw new IOException("Request body exceeds " + MAX_REQUEST_BYTES + " bytes");
                    return n;
                }

                @Override public boolean isFinished() { return delegate.isFinished(); }
                @Override public boolean isReady() { return delegate.isReady(); }
                @Override public void setReadListener(ReadListener listener) { delegate.setReadListener(listener); }
            };
        }
    }
}
