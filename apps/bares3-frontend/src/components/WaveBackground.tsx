import { useEffect, useRef } from 'react';
import * as THREE from 'three';

export function WaveBackground() {
  const mountRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!mountRef.current) return;

    const scene = new THREE.Scene();
    const camera = new THREE.PerspectiveCamera(75, mountRef.current.clientWidth / mountRef.current.clientHeight, 0.1, 1000);
    camera.position.set(0, 12, 24);
    camera.lookAt(0, 0, 0);

    const renderer = new THREE.WebGLRenderer({ alpha: true, antialias: true });
    renderer.setSize(mountRef.current.clientWidth, mountRef.current.clientHeight);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    mountRef.current.appendChild(renderer.domElement);

    const xCount = 80;
    const zCount = 30;
    const spacing = 1.3;
    const particleCount = xCount * zCount;

    const particleGeometry = new THREE.BufferGeometry();
    const positions = new Float32Array(particleCount * 3);
    const colors = new Float32Array(particleCount * 3);

    let i = 0;
    for (let iz = 0; iz < zCount; iz++) {
      for (let ix = 0; ix < xCount; ix++) {
        positions[i * 3] = (ix - xCount / 2) * spacing;
        positions[i * 3 + 1] = 0;
        positions[i * 3 + 2] = (iz - zCount / 2) * spacing;
        i++;
      }
    }
    particleGeometry.setAttribute('position', new THREE.BufferAttribute(positions, 3));
    particleGeometry.setAttribute('color', new THREE.BufferAttribute(colors, 3)); // 绑定颜色

    const material = new THREE.PointsMaterial({
      size: 0.18,
      transparent: true,
      opacity: 0.8,
      blending: THREE.AdditiveBlending,
      vertexColors: true,
    });

    const linesGeometry = new THREE.BufferGeometry();
    const linePositions = new Float32Array((xCount - 1) * zCount * 6);
    const lineColors = new Float32Array((xCount - 1) * zCount * 6); // 新增：线条颜色数组

    let lineIdx = 0;
    for (let iz = 0; iz < zCount; iz++) {
      for (let ix = 0; ix < xCount - 1; ix++) {
        const currentIdx = iz * xCount + ix;
        const nextIdx = currentIdx + 1;

        linePositions[lineIdx * 3] = positions[currentIdx * 3];
        linePositions[lineIdx * 3 + 1] = positions[currentIdx * 3 + 1];
        linePositions[lineIdx * 3 + 2] = positions[currentIdx * 3 + 2];

        linePositions[lineIdx * 3 + 3] = positions[nextIdx * 3];
        linePositions[lineIdx * 3 + 4] = positions[nextIdx * 3 + 1];
        linePositions[lineIdx * 3 + 5] = positions[nextIdx * 3 + 2];
        lineIdx += 2;
      }
    }
    linesGeometry.setAttribute('position', new THREE.BufferAttribute(linePositions, 3));
    linesGeometry.setAttribute('color', new THREE.BufferAttribute(lineColors, 3));

    const linesMaterial = new THREE.LineBasicMaterial({
      transparent: true,
      opacity: 0.3,
      blending: THREE.AdditiveBlending,
      vertexColors: true,
    });

    const lines = new THREE.LineSegments(linesGeometry, linesMaterial);
    scene.add(lines);
    const particles = new THREE.Points(particleGeometry, material);
    scene.add(particles);

    const colorBase = new THREE.Color();
    const colorPeak = new THREE.Color();
    const tempColor = new THREE.Color();

    const updateTheme = () => {
      const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
      if (isDark) {
        colorBase.setHex(0x0f172a);
        colorPeak.setHex(0x22d3ee);
        material.opacity = 0.9;
        linesMaterial.opacity = 0.25;
      } else {
        colorBase.setHex(0x94a3b8);
        colorPeak.setHex(0x0f766e);
        material.opacity = 0.7;
        linesMaterial.opacity = 0.15;
      }
    };

    updateTheme();
    const observer = new MutationObserver((m) => m.forEach(mut => mut.attributeName === 'data-theme' && updateTheme()));
    observer.observe(document.documentElement, { attributes: true });

    let animationFrameId: number;
    let time = 0;

    const animate = () => {
      animationFrameId = requestAnimationFrame(animate);
      time += 0.015;

      const pos = particleGeometry.attributes.position.array as Float32Array;
      const cPos = particleGeometry.attributes.color.array as Float32Array;
      const lPos = linesGeometry.attributes.position.array as Float32Array;
      const lcPos = linesGeometry.attributes.color.array as Float32Array;

      let pIdx = 0;
      let lIdx = 0;

      for (let iz = 0; iz < zCount; iz++) {
        for (let ix = 0; ix < xCount; ix++) {
          const x = (ix - xCount / 2) * spacing;
          const z = (iz - zCount / 2) * spacing;

          const y = Math.sin(x * 0.15 + time) * 1.8
              + Math.cos(z * 0.12 + time * 0.8) * 1.5
              - Math.sin((x + z) * 0.05 - time * 0.5) * 1.2
              + Math.cos(x * 0.3 + z * 0.2 + time) * 0.5;

          pos[pIdx * 3 + 1] = y;

          const normalizedY = Math.max(0, Math.min(1, (y + 3) / 6));
          tempColor.copy(colorBase).lerp(colorPeak, normalizedY);

          cPos[pIdx * 3] = tempColor.r;
          cPos[pIdx * 3 + 1] = tempColor.g;
          cPos[pIdx * 3 + 2] = tempColor.b;

          if (ix < xCount - 1) {
            lPos[lIdx * 6 + 1] = y;
            lcPos[lIdx * 6] = tempColor.r;
            lcPos[lIdx * 6 + 1] = tempColor.g;
            lcPos[lIdx * 6 + 2] = tempColor.b;

            const nextX = (ix + 1 - xCount / 2) * spacing;
            const yNext = Math.sin(nextX * 0.15 + time) * 1.8
                + Math.cos(z * 0.12 + time * 0.8) * 1.5
                - Math.sin((nextX + z) * 0.05 - time * 0.5) * 1.2
                + Math.cos(nextX * 0.3 + z * 0.2 + time) * 0.5;

            lPos[lIdx * 6 + 4] = yNext;

            const nextNormY = Math.max(0, Math.min(1, (yNext + 3) / 6));
            const nextTempColor = new THREE.Color().copy(colorBase).lerp(colorPeak, nextNormY);

            lcPos[lIdx * 6 + 3] = nextTempColor.r;
            lcPos[lIdx * 6 + 4] = nextTempColor.g;
            lcPos[lIdx * 6 + 5] = nextTempColor.b;

            lIdx++;
          }
          pIdx++;
        }
      }

      particleGeometry.attributes.position.needsUpdate = true;
      particleGeometry.attributes.color.needsUpdate = true;
      linesGeometry.attributes.position.needsUpdate = true;
      linesGeometry.attributes.color.needsUpdate = true;

      scene.rotation.y = Math.sin(time * 0.1) * 0.03;
      renderer.render(scene, camera);
    };

    animate();

    const handleResize = () => {
      if (!mountRef.current) return;
      camera.aspect = mountRef.current.clientWidth / mountRef.current.clientHeight;
      camera.updateProjectionMatrix();
      renderer.setSize(mountRef.current.clientWidth, mountRef.current.clientHeight);
    };
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      cancelAnimationFrame(animationFrameId);
      observer.disconnect();
      if (mountRef.current && renderer.domElement.parentNode === mountRef.current) {
        mountRef.current.removeChild(renderer.domElement);
      }
      particleGeometry.dispose();
      material.dispose();
      linesGeometry.dispose();
      linesMaterial.dispose();
      renderer.dispose();
    };
  }, []);

  return (
      <div
          ref={mountRef}
          style={{
            position: 'absolute',
            top: 0,
            left: 0,
            width: '100%',
            height: '100%',
            zIndex: 0,
            pointerEvents: 'none',
            maskImage: 'linear-gradient(to bottom, transparent 5%, black 40%, black 80%, transparent 100%)',
            WebkitMaskImage: 'linear-gradient(to bottom, transparent 5%, black 40%, black 80%, transparent 100%)',
          }}
      />
  );
}