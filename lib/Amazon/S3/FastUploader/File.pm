package Amazon::S3::FastUploader::File;
use strict;
use warnings;
use base qw( Class::Accessor );
__PACKAGE__->mk_accessors( qw(local_path remote_dir bucket config) );

our $VERSION = '0.01';

sub new {
    my $class = shift;
    my $path = shift;
    my $remote_dir = shift;
    my $bucket = shift;
    my $config = shift;

    bless {
        local_path => $path,
        remote_dir => $remote_dir,
        bucket => $bucket,
        config => $config,
    }, $class;
}

sub upload {
    my $self = shift;

    my $bucket = $self->{bucket};

    my $opt = {};
    if ($self->config->{encrypt}) {
        $opt = { 'x-amz-server-side-encryption' => 'AES256'};
    }

    my $count_failed = 0;
    my $max_retry = 5;
    my $is_success = 0;

    while (! $is_success && $count_failed < $max_retry) {
        $is_success = $bucket->add_key_filename($self->remote_key, $self->local_path, $opt) 
                or do { warn "canno upload file " . $self->from_to; $count_failed++; };
        if ($is_success) {
                return 1;
        }
    }

    die "upload failed " . $self->from_to;

}

sub from_to {
    my $self = shift;

    return $self->local_path . " -> " . $self->remote_key;
}

sub remote_path {
    my $self = shift;
    my $local_path = $self->{local_path};
    $local_path =~ s|^\./||;
    return $self->remote_dir . $local_path;
}


sub remote_key {
    my $self = shift;
    my $remote_path = $self->remote_path;
    $remote_path =~ s|^s3\://[^/]+/||i;
    $remote_path;
}

1;

__END__
