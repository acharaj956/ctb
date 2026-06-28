# Perl tooling image (CSS flat-file conversion). Build context is the repo root.
# Debian-based perl image so we can install DBD::Pg from apt (no compilation).
FROM perl:5.38

RUN apt-get update \
    && apt-get install -y --no-install-recommends libdbd-pg-perl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY tools/ /app/

# Default action: export the arrival table as a CSS 3.0 flat file.
CMD ["perl", "css_export.pl"]
