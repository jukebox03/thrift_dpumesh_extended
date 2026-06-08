project('dpumesh_dpu', 'c',
  version : '1.0.0',
  # debugoptimized = -O2 -g. The ARM control plane (dpu_worker/comch_server/dpa.c
  # — the per-RTT hot path) was previously built at -O0 (buildtype=debug), while
  # only the DPA kernel got -O2 (build_dpacc.sh). This takes effect on a FRESH
  # `meson setup`; see the -O2 add_project_arguments below for the in-place case.
  default_options : ['buildtype=debugoptimized'],
  meson_version : '>=0.61.2'
)

program_name = 'dpa_kernel'

add_project_arguments('-DDOCA_ALLOW_EXPERIMENTAL_API', language: 'c')

inc_dirs = []
inc_dirs += include_directories('/opt/mellanox/doca/include/')
inc_dirs += include_directories('..')   # for dpumesh.h

deps = []
deps += dependency('doca-common')
deps += dependency('doca-comch')
deps += dependency('doca-argp')
deps += dependency('doca-dpa')
deps += dependency('libflexio')

srcs = [
    'dpu_main.c',
    'dpu_worker.c',
    'config.c',
    'object.c',
    'common.c',
    'buffer.c',
    'ring.c',
    'comch_common.c',
    'comch_server.c',
    'comch_client.c',
    'comch_consumer.c',
    'comch_msgq.c',
    'dpa.c',
]

# Build DPA kernel via DPACC
dpacc_build_script = files('build_dpacc.sh')
device_src = files('device/dpa_kernel.c')
device_hdr = files('dpa_common.h')
dpacc_mcpu_flag = 'nv-dpa-bf3'
doca_dep = dependency('doca-dpa')
doca_lib_dir = doca_dep.get_variable(pkgconfig : 'libdir')

dpacc_lib = custom_target('dpacc_dpa_kernel',
  input : [device_src],
  output : program_name + '.a',
  command : [
    dpacc_build_script,
    meson.current_build_dir(),
    meson.current_source_dir(),
    '@INPUT@',
    program_name,
    dpacc_mcpu_flag,
    doca_lib_dir,
    '@OUTPUT@',
  ],
  depend_files : [device_hdr],
  build_by_default : true,
)

dpa_program_lib_path = meson.current_build_dir() / (program_name + '.a')

executable('dpumesh_dpu', srcs,
  dependencies : deps,
  link_args : [dpa_program_lib_path],
  include_directories : inc_dirs,
  link_depends : [dpacc_lib],
  install : false,
)
